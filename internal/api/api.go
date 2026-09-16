// Package api is Relay's HTTP surface: submit a task, get an id back
// immediately, poll status by id. All validation happens here, before a spec
// ever reaches the dispatcher — the dispatcher and store don't know an HTTP
// request exists.
package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/roneettopiwala/relay/internal/dispatch"
	"github.com/roneettopiwala/relay/internal/store"
	"github.com/roneettopiwala/relay/internal/task"
)

// maxBodyBytes caps a request body so a client can't hand us an unbounded
// payload to decode.
const maxBodyBytes = 1 << 20 // 1 MiB

// Limits are the server-side ceilings a submitted task's resource request is
// validated against.
type Limits struct {
	MaxCPU      float64
	MaxMemBytes int64
	MaxTimeout  time.Duration
}

// API wires the store, dispatcher, and dead-letter queue to HTTP handlers.
type API struct {
	store      *store.Store
	dispatcher *dispatch.Dispatcher
	deadLetter dispatch.DeadLetterQueue // may be nil: GET /dead-letters then reports empty
	defaults   task.Defaults
	limits     Limits
}

// New builds an API. defaults fills in whatever a submitted spec omits;
// limits are the ceilings it's validated against.
func New(st *store.Store, d *dispatch.Dispatcher, dlq dispatch.DeadLetterQueue, defaults task.Defaults, limits Limits) *API {
	return &API{
		store:      st,
		dispatcher: d,
		deadLetter: dlq,
		defaults:   defaults,
		limits:     limits,
	}
}

// Routes builds the handler tree. Go 1.22's method-aware ServeMux patterns
// mean a right-path-wrong-method request (e.g. GET /tasks) gets an automatic
// 405, with no extra code here.
func (a *API) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /tasks", a.handleSubmit)
	mux.HandleFunc("GET /tasks/{id}", a.handleGet)
	mux.HandleFunc("GET /stats", a.handleStats)
	mux.HandleFunc("GET /dead-letters", a.handleDeadLetters)
	mux.HandleFunc("GET /healthz", a.handleHealthz)
	return mux
}

// submitRequest is the POST /tasks body. CPUs and TimeoutSeconds are pointers
// so "the client didn't send this" (nil, defaults apply) is distinguishable
// from "the client sent an invalid value" (e.g. 0 or negative, rejected).
type submitRequest struct {
	Image          string   `json:"image"`
	Cmd            []string `json:"cmd"`
	CPUs           *float64 `json:"cpus"`
	Memory         string   `json:"memory"`
	TimeoutSeconds *int     `json:"timeout_seconds"`
}

func (a *API) handleSubmit(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)

	var req submitRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body: "+err.Error())
		return
	}
	if len(req.Cmd) == 0 {
		writeError(w, http.StatusBadRequest, "cmd must be a non-empty array")
		return
	}

	spec := task.Spec{
		Image:    strings.TrimSpace(req.Image),
		Cmd:      req.Cmd,
		MemLimit: strings.TrimSpace(req.Memory),
	}

	if req.CPUs != nil {
		switch {
		case *req.CPUs <= 0:
			writeError(w, http.StatusBadRequest, "cpus must be positive")
			return
		case *req.CPUs > a.limits.MaxCPU:
			writeError(w, http.StatusBadRequest, fmt.Sprintf("cpus %g exceeds server maximum of %g", *req.CPUs, a.limits.MaxCPU))
			return
		}
		spec.CPULimit = *req.CPUs
	}

	if spec.MemLimit != "" {
		bytes, err := ParseMemBytes(spec.MemLimit)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid memory format: "+err.Error())
			return
		}
		if bytes > a.limits.MaxMemBytes {
			writeError(w, http.StatusBadRequest, fmt.Sprintf("memory %q exceeds server maximum of %d bytes", spec.MemLimit, a.limits.MaxMemBytes))
			return
		}
	}

	if req.TimeoutSeconds != nil {
		if *req.TimeoutSeconds <= 0 {
			writeError(w, http.StatusBadRequest, "timeout_seconds must be positive")
			return
		}
		timeout := time.Duration(*req.TimeoutSeconds) * time.Second
		if timeout > a.limits.MaxTimeout {
			writeError(w, http.StatusBadRequest, fmt.Sprintf("timeout_seconds %d exceeds server maximum of %s", *req.TimeoutSeconds, a.limits.MaxTimeout))
			return
		}
		spec.Timeout = timeout
	}

	spec = spec.WithDefaults(a.defaults)

	t, err := a.dispatcher.Submit(spec)
	if err != nil {
		// Every failure Submit reports — shutting down, overloaded, queue
		// unreachable — is the same kind of thing from a client's point of
		// view: try again shortly. Retry-After makes that explicit instead of
		// leaving the client to guess a backoff.
		w.Header().Set("Retry-After", "1")
		writeError(w, http.StatusServiceUnavailable, err.Error())
		return
	}
	writeJSON(w, http.StatusAccepted, t)
}

func (a *API) handleGet(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	t, ok := a.store.Get(id)
	if !ok {
		writeError(w, http.StatusNotFound, "task not found")
		return
	}
	writeJSON(w, http.StatusOK, t)
}

// statsResponse is the Phase 5 dashboard seam. Most of it (queue depth,
// worker utilization) is already tracked by store.Stats and the dispatcher's
// pool — this handler is just arithmetic, not new state. The latency fields
// come from the dispatcher's rolling window (internal/metrics): a live
// snapshot, not history — the dashboard builds its own trend by polling this
// repeatedly, so Relay never needs to store a time-series itself.
type statsResponse struct {
	Pending      int `json:"pending"`
	Running      int `json:"running"`
	Completed    int `json:"completed"`
	Failed       int `json:"failed"`
	WorkersTotal int `json:"workers_total"`
	WorkersBusy  int `json:"workers_busy"`

	// P50LatencyMs/P99LatencyMs/LatencySampleCount are 0 until at least one
	// task has reached a terminal state — the dashboard should treat a
	// sample count of 0 as "no data yet", not "0ms latency".
	P50LatencyMs       int64 `json:"p50_latency_ms"`
	P99LatencyMs       int64 `json:"p99_latency_ms"`
	LatencySampleCount int   `json:"latency_sample_count"`
}

func (a *API) handleStats(w http.ResponseWriter, r *http.Request) {
	s := a.store.Stats()
	total := a.dispatcher.WorkersTotal()
	idle := a.dispatcher.WorkersIdle()
	m := a.dispatcher.MetricsSnapshot()
	writeJSON(w, http.StatusOK, statsResponse{
		Pending:            s.Pending,
		Running:            s.Running,
		Completed:          s.Completed,
		Failed:             s.Failed,
		WorkersTotal:       total,
		WorkersBusy:        total - idle,
		P50LatencyMs:       m.P50.Milliseconds(),
		P99LatencyMs:       m.P99.Milliseconds(),
		LatencySampleCount: m.SampleCount,
	})
}

// deadLettersLimit caps how many entries a single GET /dead-letters call
// returns. There's no pagination yet — Phase 2 scope is "make failures
// inspectable at all", not a full admin API.
const deadLettersLimit = 100

type deadLettersResponse struct {
	Entries []dispatch.DeadLetterEntry `json:"entries"`
}

func (a *API) handleDeadLetters(w http.ResponseWriter, r *http.Request) {
	if a.deadLetter == nil {
		writeJSON(w, http.StatusOK, deadLettersResponse{Entries: []dispatch.DeadLetterEntry{}})
		return
	}
	entries, err := a.deadLetter.List(r.Context(), deadLettersLimit)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "list dead letters: "+err.Error())
		return
	}
	writeJSON(w, http.StatusOK, deadLettersResponse{Entries: entries})
}

func (a *API) handleHealthz(w http.ResponseWriter, r *http.Request) {
	// The Docker daemon was already checked once at startup (see
	// executor.NewDockerExecutor); re-checking it on every health probe would
	// just add latency for no real benefit in Phase 1.
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

// memPattern matches Docker's own --memory shorthand: a number optionally
// followed by a b/k/m/g unit (case-insensitive), e.g. "256m", "1g", "512".
var memPattern = regexp.MustCompile(`(?i)^(\d+)([bkmg]?)$`)

// ParseMemBytes parses a Docker-style memory string into bytes. Exported so
// main.go can validate the -max-memory flag with the same parser used to
// validate a client's request — one implementation, not two.
func ParseMemBytes(s string) (int64, error) {
	m := memPattern.FindStringSubmatch(strings.TrimSpace(s))
	if m == nil {
		return 0, fmt.Errorf("expected a number optionally followed by b/k/m/g, got %q", s)
	}
	n, err := strconv.ParseInt(m[1], 10, 64)
	if err != nil {
		return 0, err
	}
	switch strings.ToLower(m[2]) {
	case "", "b":
		return n, nil
	case "k":
		return n * 1024, nil
	case "m":
		return n * 1024 * 1024, nil
	case "g":
		return n * 1024 * 1024 * 1024, nil
	default:
		return 0, fmt.Errorf("unknown memory unit in %q", s)
	}
}
