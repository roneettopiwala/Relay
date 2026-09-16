// Command loadtest fires many concurrent requests at a running Relay server
// to prove dispatch stays safe under load: no lost tasks, no task left
// non-terminal, and worker concurrency never exceeds the configured pool.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/roneettopiwala/relay/internal/relayclient"
)

type statsResp struct {
	Pending      int `json:"pending"`
	Running      int `json:"running"`
	Completed    int `json:"completed"`
	Failed       int `json:"failed"`
	WorkersTotal int `json:"workers_total"`
	WorkersBusy  int `json:"workers_busy"`
}

type submitResult struct {
	id         string
	statusCode int
	err        error
	latency    time.Duration
	submitAt   time.Time
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

	ctx := context.Background()

	// The default transport keeps only 2 idle connections per host, which
	// would serialize most of a high -c run at the TCP layer and make this
	// tool the bottleneck instead of the server. Give it enough headroom to
	// actually sustain the concurrency it's supposed to be generating.
	httpClient := &http.Client{
		Timeout:   15 * time.Second,
		Transport: &http.Transport{MaxIdleConnsPerHost: *c + 1},
	}
	rc := relayclient.New(*url, httpClient)

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
				s, err := getStats(httpClient, *url)
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
	body := relayclient.SubmitRequest{Image: *image, Cmd: []string{"sh", "-c", *cmdStr}, TimeoutSeconds: timeoutSec}
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
				t, err := rc.Submit(ctx, body)
				statusCode := 0
				var apiErr *relayclient.APIError
				if errors.As(err, &apiErr) {
					statusCode = apiErr.StatusCode
				}
				results[idx] = submitResult{
					id: t.ID, statusCode: statusCode, err: err,
					latency: time.Since(start), submitAt: start,
				}
			}
		}()
	}
	subWG.Wait()
	submitDuration := time.Since(wallStart)

	var submitOK, submitRejected, submitErrs int
	submitLatencies := make([]time.Duration, 0, *n)
	ids := make([]string, 0, *n)
	submitAt := make(map[string]time.Time, *n)
	for _, r := range results {
		submitLatencies = append(submitLatencies, r.latency)
		switch {
		case r.err == nil:
			submitOK++
			ids = append(ids, r.id)
			submitAt[r.id] = r.submitAt
		case r.statusCode == http.StatusServiceUnavailable:
			// Backpressure doing its job, not a bug — the server explicitly
			// shed this request rather than accept an unbounded backlog.
			submitRejected++
		default:
			submitErrs++
		}
	}
	fmt.Printf("submitted %d/%d ok (%d rejected by backpressure, %d other errors) in %s\n",
		submitOK, *n, submitRejected, submitErrs, submitDuration)

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
				t, err := rc.WaitForTerminal(ctx, id, *pollInterval, *pollTimeout)
				if err != nil {
					// ErrPollTimeout ("stuck") and a genuine request error are
					// both treated as stuck here — either way this id never
					// resolved to a terminal state within the budget.
					atomic.AddInt32(&stuck, 1)
					continue
				}
				switch t.Status {
				case "completed":
					atomic.AddInt32(&completed, 1)
				case "failed":
					atomic.AddInt32(&failed, 1)
				}
				lat := time.Since(submitAt[id])
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
	fmt.Printf("rejected (503, backpressure): %d\n", submitRejected)
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

// getStats is loadtest-specific (worker-utilization sampling isn't part of
// relayclient's submit/poll job), so it stays a small local helper.
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
