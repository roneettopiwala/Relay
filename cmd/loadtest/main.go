// Command loadtest fires many concurrent requests at a running Relay server
// to prove dispatch stays safe under load: no lost tasks, no task left
// non-terminal, and worker concurrency never exceeds the configured pool.
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

type submitReq struct {
	Image          string   `json:"image"`
	Cmd            []string `json:"cmd"`
	TimeoutSeconds int      `json:"timeout_seconds"`
}

type taskResp struct {
	ID     string `json:"id"`
	Status string `json:"status"`
}

type statsResp struct {
	Pending      int `json:"pending"`
	Running      int `json:"running"`
	Completed    int `json:"completed"`
	Failed       int `json:"failed"`
	WorkersTotal int `json:"workers_total"`
	WorkersBusy  int `json:"workers_busy"`
}

type submitResult struct {
	id       string
	err      error
	latency  time.Duration
	submitAt time.Time
}

func main() {
	url := flag.String("url", "http://localhost:8080", "Relay server base URL")
	n := flag.Int("n", 200, "total tasks to submit")
	c := flag.Int("c", 50, "concurrent submitters/pollers")
	image := flag.String("image", "alpine", "container image")
	cmdStr := flag.String("cmd", "sleep 1", "shell command run inside the container (via sh -c)")
	timeoutSec := flag.Int("timeout", 10, "per-task timeout_seconds sent with each request")
	pollInterval := flag.Duration("poll-interval", 20*time.Millisecond, "how often to poll a task's status")
	// 60s covers typical runs, but a deep backlog (n far exceeding workers *
	// how many tasks fit in the run) pushes a late task's wait time well past
	// that — size this explicitly as roughly (n / workers) * per-task time
	// for a heavy run, or a "stuck" count here is likely just this timeout
	// being too short, not a lost task (check /stats before assuming a bug).
	pollTimeout := flag.Duration("poll-timeout", 60*time.Second, "max time to wait for one task to reach a terminal state")
	statsInterval := flag.Duration("stats-interval", 50*time.Millisecond, "how often to sample /stats for worker utilization")
	flag.Parse()

	// The default transport keeps only 2 idle connections per host, which
	// would serialize most of a high -c run at the TCP layer and make this
	// tool the bottleneck instead of the server. Give it enough headroom to
	// actually sustain the concurrency it's supposed to be generating.
	client := &http.Client{
		Timeout:   15 * time.Second,
		Transport: &http.Transport{MaxIdleConnsPerHost: *c + 1},
	}

	fmt.Printf("Relay load test: n=%d concurrency=%d url=%s cmd=%q\n", *n, *c, *url, *cmdStr)

	// Stats sampler runs for the whole test, tracking peak worker utilization
	// independently of submit/poll timing.
	stopStats := make(chan struct{})
	var peakBusy, workersTotal int32
	var statsWG sync.WaitGroup
	statsWG.Add(1)
	go func() {
		defer statsWG.Done()
		ticker := time.NewTicker(*statsInterval)
		defer ticker.Stop()
		for {
			select {
			case <-stopStats:
				return
			case <-ticker.C:
				s, err := getStats(client, *url)
				if err != nil {
					continue
				}
				atomic.StoreInt32(&workersTotal, int32(s.WorkersTotal))
				for {
					cur := atomic.LoadInt32(&peakBusy)
					if int32(s.WorkersBusy) <= cur {
						break
					}
					if atomic.CompareAndSwapInt32(&peakBusy, cur, int32(s.WorkersBusy)) {
						break
					}
				}
			}
		}
	}()

	// --- Phase A: submit all n tasks through c concurrent goroutines ---
	body := submitReq{Image: *image, Cmd: []string{"sh", "-c", *cmdStr}, TimeoutSeconds: *timeoutSec}
	jobs := make(chan int, *n)
	for i := 0; i < *n; i++ {
		jobs <- i
	}
	close(jobs)

	results := make([]submitResult, *n)
	var subWG sync.WaitGroup
	wallStart := time.Now()
	for w := 0; w < *c; w++ {
		subWG.Add(1)
		go func() {
			defer subWG.Done()
			for idx := range jobs {
				start := time.Now()
				id, err := submitTask(client, *url, body)
				results[idx] = submitResult{id: id, err: err, latency: time.Since(start), submitAt: start}
			}
		}()
	}
	subWG.Wait()
	submitDuration := time.Since(wallStart)

	var submitOK, submitErrs int
	submitLatencies := make([]time.Duration, 0, *n)
	ids := make([]string, 0, *n)
	submitAt := make(map[string]time.Time, *n)
	for _, r := range results {
		submitLatencies = append(submitLatencies, r.latency)
		if r.err != nil {
			submitErrs++
			continue
		}
		submitOK++
		ids = append(ids, r.id)
		submitAt[r.id] = r.submitAt
	}
	fmt.Printf("submitted %d/%d ok (%d errors) in %s\n", submitOK, *n, submitErrs, submitDuration)

	// --- Phase B: poll every submitted task to a terminal state ---
	pollConcurrency := *c
	if pollConcurrency > len(ids) {
		pollConcurrency = len(ids)
	}
	if pollConcurrency == 0 {
		pollConcurrency = 1
	}
	pollJobs := make(chan string, len(ids))
	for _, id := range ids {
		pollJobs <- id
	}
	close(pollJobs)

	var completed, failed, stuck int32
	var e2eMu sync.Mutex
	e2eLatencies := make([]time.Duration, 0, len(ids))
	var pollWG sync.WaitGroup
	for w := 0; w < pollConcurrency; w++ {
		pollWG.Add(1)
		go func() {
			defer pollWG.Done()
			for id := range pollJobs {
				status, terminalAt, isStuck := pollUntilTerminal(client, *url, id, *pollInterval, *pollTimeout)
				if isStuck {
					atomic.AddInt32(&stuck, 1)
					continue
				}
				switch status {
				case "completed":
					atomic.AddInt32(&completed, 1)
				case "failed":
					atomic.AddInt32(&failed, 1)
				}
				lat := terminalAt.Sub(submitAt[id])
				e2eMu.Lock()
				e2eLatencies = append(e2eLatencies, lat)
				e2eMu.Unlock()
			}
		}()
	}
	pollWG.Wait()
	close(stopStats)
	statsWG.Wait()
	totalDuration := time.Since(wallStart)

	finalCompleted := atomic.LoadInt32(&completed)
	finalFailed := atomic.LoadInt32(&failed)
	finalStuck := atomic.LoadInt32(&stuck)
	finalPeakBusy := atomic.LoadInt32(&peakBusy)
	finalWorkersTotal := atomic.LoadInt32(&workersTotal)

	p50Sub, p99Sub := percentiles(submitLatencies)
	p50E2E, p99E2E := percentiles(e2eLatencies)
	throughput := 0.0
	if totalDuration > 0 {
		throughput = float64(submitOK) / totalDuration.Seconds()
	}
	failureRate := 0.0
	if submitOK > 0 {
		failureRate = float64(int(finalFailed)+int(finalStuck)) / float64(submitOK) * 100
	}

	fmt.Println()
	fmt.Println("=== Relay Load Test Report ===")
	fmt.Printf("requested:          %d\n", *n)
	fmt.Printf("submitted (202):    %d\n", submitOK)
	fmt.Printf("submit errors:      %d\n", submitErrs)
	fmt.Printf("completed:          %d\n", finalCompleted)
	fmt.Printf("failed:             %d\n", finalFailed)
	fmt.Printf("stuck (never terminal): %d\n", finalStuck)
	fmt.Printf("submit latency:     p50=%s p99=%s\n", p50Sub, p99Sub)
	fmt.Printf("end-to-end latency: p50=%s p99=%s\n", p50E2E, p99E2E)
	fmt.Printf("throughput:         %.2f tasks/sec\n", throughput)
	fmt.Printf("workers total:      %d\n", finalWorkersTotal)
	fmt.Printf("peak workers busy:  %d\n", finalPeakBusy)
	fmt.Printf("failure rate:       %.1f%%\n", failureRate)

	ok := submitErrs == 0 &&
		finalStuck == 0 &&
		int(finalCompleted)+int(finalFailed) == submitOK &&
		(finalWorkersTotal == 0 || finalPeakBusy <= finalWorkersTotal)

	if ok {
		fmt.Println("\nPASS: every submitted task reached a terminal state; peak concurrency never exceeded the worker pool")
		return
	}
	fmt.Println("\nFAIL: see counts above")
	os.Exit(1)
}

func submitTask(client *http.Client, base string, body submitReq) (string, error) {
	b, err := json.Marshal(body)
	if err != nil {
		return "", err
	}
	resp, err := client.Post(base+"/tasks", "application/json", bytes.NewReader(b))
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusAccepted {
		return "", fmt.Errorf("status %d: %s", resp.StatusCode, data)
	}
	var t taskResp
	if err := json.Unmarshal(data, &t); err != nil {
		return "", err
	}
	return t.ID, nil
}

// pollUntilTerminal polls until the task is completed/failed or pollTimeout
// passes, in which case stuck is true — that's the "lost or hung task" signal
// the load test's PASS/FAIL check treats as a hard failure.
func pollUntilTerminal(client *http.Client, base, id string, interval, pollTimeout time.Duration) (status string, terminalAt time.Time, stuck bool) {
	deadline := time.Now().Add(pollTimeout)
	for {
		if resp, err := client.Get(base + "/tasks/" + id); err == nil {
			data, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			var t taskResp
			if json.Unmarshal(data, &t) == nil && (t.Status == "completed" || t.Status == "failed") {
				return t.Status, time.Now(), false
			}
		}
		if time.Now().After(deadline) {
			return "", time.Time{}, true
		}
		time.Sleep(interval)
	}
}

func getStats(client *http.Client, base string) (statsResp, error) {
	resp, err := client.Get(base + "/stats")
	if err != nil {
		return statsResp{}, err
	}
	defer resp.Body.Close()
	var s statsResp
	if err := json.NewDecoder(resp.Body).Decode(&s); err != nil {
		return statsResp{}, err
	}
	return s, nil
}

// percentiles uses the nearest-rank method — simple and good enough for a
// load-test summary, not a metrics system.
func percentiles(d []time.Duration) (p50, p99 time.Duration) {
	if len(d) == 0 {
		return 0, 0
	}
	sorted := append([]time.Duration(nil), d...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	at := func(p float64) time.Duration {
		i := int(p * float64(len(sorted)-1))
		if i < 0 {
			i = 0
		}
		if i >= len(sorted) {
			i = len(sorted) - 1
		}
		return sorted[i]
	}
	return at(0.50), at(0.99)
}
