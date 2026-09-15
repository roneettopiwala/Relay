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

// API wires the store and dispatcher to HTTP handlers, plus the server-side
// limits a submitted task's resource requests are checked against.
type API struct {
	store      *store.Store
	dispatcher *dispatch.Dispatcher
	defaults   task.Defaults

	maxCPU      float64
	maxMemBytes int64
	maxTimeout  time.Duration
}

// New builds an API. maxCPU/maxMemBytes/maxTimeout are the ceilings a client's
// request is validated against; defaults fill in whatever a request omits.
func New(st *store.Store, d *dispatch.Dispatcher, defaults task.Defaults, maxCPU float64, maxMemBytes int64, maxTimeout time.Duration) *API {
	return &API{
		store:       st,
		dispatcher:  d,
		defaults:    defaults,
		maxCPU:      maxCPU,
		maxMemBytes: maxMemBytes,
		maxTimeout:  maxTimeout,
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
		case *req.CPUs > a.maxCPU:
			writeError(w, http.StatusBadRequest, fmt.Sprintf("cpus %g exceeds server maximum of %g", *req.CPUs, a.maxCPU))
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
		if bytes > a.maxMemBytes {
			writeError(w, http.StatusBadRequest, fmt.Sprintf("memory %q exceeds server maximum of %d bytes", spec.MemLimit, a.maxMemBytes))
			return
		}
	}

	if req.TimeoutSeconds != nil {
		if *req.TimeoutSeconds <= 0 {
			writeError(w, http.StatusBadRequest, "timeout_seconds must be positive")
			return
		}
		timeout := time.Duration(*req.TimeoutSeconds) * time.Second
		if timeout > a.maxTimeout {
			writeError(w, http.StatusBadRequest, fmt.Sprintf("timeout_seconds %d exceeds server maximum of %s", *req.TimeoutSeconds, a.maxTimeout))
			return
		}
		spec.Timeout = timeout
	}

	spec = spec.WithDefaults(a.defaults)

	t, err := a.dispatcher.Submit(spec)
	if err != nil {
		// The only failure Submit reports is dispatch.ErrShuttingDown.
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

// statsResponse is the Phase 5 dashboard seam: everything it needs (queue
// depth, worker utilization) is already tracked by store.Stats and the
// dispatcher's pool, so this handler is just arithmetic, not new state.
type statsResponse struct {
	Pending      int `json:"pending"`
	Running      int `json:"running"`
	Completed    int `json:"completed"`
	Failed       int `json:"failed"`
	WorkersTotal int `json:"workers_total"`
	WorkersBusy  int `json:"workers_busy"`
}

func (a *API) handleStats(w http.ResponseWriter, r *http.Request) {
	s := a.store.Stats()
	total := a.dispatcher.WorkersTotal()
	idle := a.dispatcher.WorkersIdle()
	writeJSON(w, http.StatusOK, statsResponse{
		Pending:      s.Pending,
		Running:      s.Running,
		Completed:    s.Completed,
		Failed:       s.Failed,
		WorkersTotal: total,
		WorkersBusy:  total - idle,
	})
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
