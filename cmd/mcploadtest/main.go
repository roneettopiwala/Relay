// Command mcploadtest fires many concurrent MCP sessions at a running Relay
// server to prove the MCP integration itself is safe under the traffic
// shape Phase 4 cares about: many independent agent sessions, each making
// its own sequential tool calls, all running concurrently — not one big
// parallel burst of HTTP requests like cmd/loadtest, but N separate
// mcpserver subprocesses each behaving like its own Claude Code session.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

type statsResp struct {
	Pending      int `json:"pending"`
	Running      int `json:"running"`
	Completed    int `json:"completed"`
	Failed       int `json:"failed"`
	WorkersTotal int `json:"workers_total"`
	WorkersBusy  int `json:"workers_busy"`
}

func main() {
	relayURL := flag.String("relay-url", "http://localhost:8080", "Relay server base URL")
	mcpserverBin := flag.String("mcpserver-bin", "", "path to a pre-built mcpserver binary (required — "+
		"go run would recompile per session, measuring the Go toolchain instead of Relay)")
	sessions := flag.Int("sessions", 10, "number of concurrent MCP sessions, each its own subprocess + connection")
	callsPerSession := flag.Int("calls-per-session", 5, "sequential run_task calls each session makes before closing")
	image := flag.String("image", "alpine", "container image")
	cmdStr := flag.String("cmd", "sleep 0.2", "shell command run inside the container (via sh -c)")
	timeoutSec := flag.Int("timeout", 10, "per-task timeout_seconds sent with each call")
	statsInterval := flag.Duration("stats-interval", 50*time.Millisecond, "how often to sample /stats for worker utilization")
	flag.Parse()

	if *mcpserverBin == "" {
		fmt.Fprintln(os.Stderr, "mcploadtest: -mcpserver-bin is required")
		fmt.Fprintln(os.Stderr, "build one with: go build -o /tmp/mcpserver ./cmd/mcpserver")
		os.Exit(2)
	}

	total := *sessions * *callsPerSession
	fmt.Printf("Relay MCP load test: sessions=%d calls/session=%d (total %d calls) url=%s cmd=%q\n",
		*sessions, *callsPerSession, total, *relayURL, *cmdStr)

	// Stats sampler, same pattern as cmd/loadtest: tracks peak worker
	// utilization independently of call timing.
	stopStats := make(chan struct{})
	var peakBusy, workersTotal int32
	var statsWG sync.WaitGroup
	statsWG.Add(1)
	go func() {
		defer statsWG.Done()
		client := &http.Client{Timeout: 5 * time.Second}
		ticker := time.NewTicker(*statsInterval)
		defer ticker.Stop()
		for {
			select {
			case <-stopStats:
				return
			case <-ticker.C:
				s, err := getStats(client, *relayURL)
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

	sessionConnectLatencies := make([]time.Duration, *sessions)
	var callMu sync.Mutex
	callLatencies := make([]time.Duration, 0, total)
	var completed, failed, sessionErrs, callErrs int32

	var wg sync.WaitGroup
	wallStart := time.Now()
	for s := 0; s < *sessions; s++ {
		wg.Add(1)
		go func(sessionIdx int) {
			defer wg.Done()

			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()

			connectStart := time.Now()
			cmd := exec.Command(*mcpserverBin, "-relay-url", *relayURL)
			transport := &mcp.CommandTransport{Command: cmd}
			client := mcp.NewClient(&mcp.Implementation{
				Name: fmt.Sprintf("mcploadtest-session-%d", sessionIdx), Version: "v0.0.0",
			}, nil)
			cs, err := client.Connect(ctx, transport, nil)
			if err != nil {
				atomic.AddInt32(&sessionErrs, 1)
				fmt.Printf("session %d: connect failed: %v\n", sessionIdx, err)
				return
			}
			defer cs.Close()
			sessionConnectLatencies[sessionIdx] = time.Since(connectStart)

			// Sequential calls within a session — an agent conversation calls
			// one tool, waits for the result, then decides what's next; it
			// doesn't fire N calls from one session in parallel. Concurrency
			// here comes from having many sessions, not many calls per session.
			for c := 0; c < *callsPerSession; c++ {
				start := time.Now()
				result, err := cs.CallTool(ctx, &mcp.CallToolParams{
					Name: "run_task",
					Arguments: map[string]any{
						"image": *image, "cmd": []string{"sh", "-c", *cmdStr}, "timeout_seconds": *timeoutSec,
					},
				})
				lat := time.Since(start)
				callMu.Lock()
				callLatencies = append(callLatencies, lat)
				callMu.Unlock()

				if err != nil {
					// A genuine MCP/transport-level error — not a task outcome
					// (see cmd/mcpserver: those come back as IsError, not err).
					atomic.AddInt32(&callErrs, 1)
					fmt.Printf("session %d call %d: CallTool error: %v\n", sessionIdx, c, err)
					continue
				}
				if result.IsError {
					atomic.AddInt32(&failed, 1)
				} else {
					atomic.AddInt32(&completed, 1)
				}
			}
		}(s)
	}
	wg.Wait()
	close(stopStats)
	statsWG.Wait()
	totalDuration := time.Since(wallStart)

	p50Connect, p99Connect := percentiles(sessionConnectLatencies)
	p50Call, p99Call := percentiles(callLatencies)
	throughput := 0.0
	if totalDuration > 0 {
		throughput = float64(completed+failed) / totalDuration.Seconds()
	}

	fmt.Println()
	fmt.Println("=== Relay MCP Load Test Report ===")
	fmt.Printf("sessions:            %d (connect errors: %d)\n", *sessions, sessionErrs)
	fmt.Printf("calls requested:     %d\n", total)
	fmt.Printf("completed:           %d\n", completed)
	fmt.Printf("failed (task-level): %d\n", failed)
	fmt.Printf("call errors (bugs):  %d\n", callErrs)
	fmt.Printf("session connect lat: p50=%s p99=%s\n", p50Connect, p99Connect)
	fmt.Printf("call latency:        p50=%s p99=%s\n", p50Call, p99Call)
	fmt.Printf("throughput:          %.2f calls/sec\n", throughput)
	fmt.Printf("workers total:       %d\n", workersTotal)
	fmt.Printf("peak workers busy:   %d\n", peakBusy)

	ok := sessionErrs == 0 &&
		callErrs == 0 &&
		int(completed+failed) == total &&
		(workersTotal == 0 || peakBusy <= workersTotal)

	if ok {
		fmt.Println("\nPASS: every session connected, every call resolved to a real outcome, " +
			"peak concurrency never exceeded the worker pool")
		return
	}
	fmt.Println("\nFAIL: see counts above")
	os.Exit(1)
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
