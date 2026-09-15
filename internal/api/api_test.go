package api_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/roneettopiwala/relay/internal/api"
	"github.com/roneettopiwala/relay/internal/dispatch"
	"github.com/roneettopiwala/relay/internal/store"
	"github.com/roneettopiwala/relay/internal/task"
)

// sleepExecutor is a controllable no-Docker test double: it just waits, so
// these tests exercise routing/validation/async-ness without any container.
type sleepExecutor struct{ delay time.Duration }

func (s sleepExecutor) Execute(ctx context.Context, t task.Task) (task.Result, error) {
	select {
	case <-time.After(s.delay):
		return task.Result{ExitCode: 0, Output: "ok"}, nil
	case <-ctx.Done():
		return task.Result{}, ctx.Err()
	}
}

func newTestAPI(t *testing.T, workers int, exec dispatch.Executor) *api.API {
	t.Helper()
	st := store.New()
	d := dispatch.New(st, workers, exec)
	t.Cleanup(func() { d.Shutdown(context.Background()) })
	defaults := task.Defaults{Image: "alpine", CPULimit: 0.5, MemLimit: "128m", Timeout: 30 * time.Second}
	return api.New(st, d, defaults, 2.0, 512*1024*1024, 60*time.Second)
}

func doJSON(t *testing.T, h http.Handler, method, path string, body any) *httptest.ResponseRecorder {
	t.Helper()
	var r io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("marshal request body: %v", err)
		}
		r = bytes.NewReader(b)
	}
	req := httptest.NewRequest(method, path, r)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func decodeTask(t *testing.T, rec *httptest.ResponseRecorder) task.Task {
	t.Helper()
	var tk task.Task
	if err := json.Unmarshal(rec.Body.Bytes(), &tk); err != nil {
		t.Fatalf("decode task: %v; body=%s", err, rec.Body.String())
	}
	return tk
}

func TestSubmit_Success(t *testing.T) {
	a := newTestAPI(t, 2, sleepExecutor{delay: 10 * time.Millisecond})
	rec := doJSON(t, a.Routes(), http.MethodPost, "/tasks", map[string]any{
		"image": "alpine", "cmd": []string{"echo", "hi"},
	})
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202; body=%s", rec.Code, rec.Body)
	}
	got := decodeTask(t, rec)
	if got.ID == "" {
		t.Error("response has empty id")
	}
	if got.Status != task.StatusPending {
		t.Errorf("status = %s, want pending", got.Status)
	}
}

func TestSubmit_MissingCmd(t *testing.T) {
	a := newTestAPI(t, 1, sleepExecutor{delay: time.Millisecond})
	rec := doJSON(t, a.Routes(), http.MethodPost, "/tasks", map[string]any{"image": "alpine"})
	if rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400; body=%s", rec.Code, rec.Body)
	}
}

func TestSubmit_MalformedJSON(t *testing.T) {
	a := newTestAPI(t, 1, sleepExecutor{delay: time.Millisecond})
	req := httptest.NewRequest(http.MethodPost, "/tasks", strings.NewReader("{not valid json"))
	rec := httptest.NewRecorder()
	a.Routes().ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400 (must not 500 or panic on bad JSON); body=%s", rec.Code, rec.Body)
	}
}

func TestSubmit_CPUsOverMax(t *testing.T) {
	a := newTestAPI(t, 1, sleepExecutor{delay: time.Millisecond})
	rec := doJSON(t, a.Routes(), http.MethodPost, "/tasks", map[string]any{
		"cmd": []string{"x"}, "cpus": 64.0,
	})
	if rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400 (client must not be able to request --cpus 64)", rec.Code)
	}
}

func TestSubmit_CPUsNonPositive(t *testing.T) {
	a := newTestAPI(t, 1, sleepExecutor{delay: time.Millisecond})
	rec := doJSON(t, a.Routes(), http.MethodPost, "/tasks", map[string]any{
		"cmd": []string{"x"}, "cpus": -1.0,
	})
	if rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", rec.Code)
	}
}

func TestSubmit_MemoryOverMax(t *testing.T) {
	a := newTestAPI(t, 1, sleepExecutor{delay: time.Millisecond})
	rec := doJSON(t, a.Routes(), http.MethodPost, "/tasks", map[string]any{
		"cmd": []string{"x"}, "memory": "4g",
	})
	if rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400 (max is 512m in this test)", rec.Code)
	}
}

func TestSubmit_InvalidMemoryFormat(t *testing.T) {
	a := newTestAPI(t, 1, sleepExecutor{delay: time.Millisecond})
	rec := doJSON(t, a.Routes(), http.MethodPost, "/tasks", map[string]any{
		"cmd": []string{"x"}, "memory": "lots",
	})
	if rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", rec.Code)
	}
}

func TestSubmit_TimeoutOverMax(t *testing.T) {
	a := newTestAPI(t, 1, sleepExecutor{delay: time.Millisecond})
	rec := doJSON(t, a.Routes(), http.MethodPost, "/tasks", map[string]any{
		"cmd": []string{"x"}, "timeout_seconds": 3600,
	})
	if rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400 (max is 60s in this test)", rec.Code)
	}
}

func TestGet_NotFound(t *testing.T) {
	a := newTestAPI(t, 1, sleepExecutor{delay: time.Millisecond})
	rec := doJSON(t, a.Routes(), http.MethodGet, "/tasks/does-not-exist", nil)
	if rec.Code != http.StatusNotFound {
		t.Errorf("status = %d, want 404", rec.Code)
	}
}

func TestGet_Found(t *testing.T) {
	a := newTestAPI(t, 1, sleepExecutor{delay: time.Millisecond})
	submitted := decodeTask(t, doJSON(t, a.Routes(), http.MethodPost, "/tasks", map[string]any{
		"cmd": []string{"x"},
	}))

	rec := doJSON(t, a.Routes(), http.MethodGet, "/tasks/"+submitted.ID, nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	got := decodeTask(t, rec)
	if got.ID != submitted.ID {
		t.Errorf("GET returned id %q, want %q", got.ID, submitted.ID)
	}
}

// TestSubmit_DoesNotBlockOnBusyWorkers is the key async-correctness check:
// with the single worker already occupied by a slow task, a second Submit
// must still return in milliseconds (queued as pending), not wait for a
// worker to free up.
func TestSubmit_DoesNotBlockOnBusyWorkers(t *testing.T) {
	a := newTestAPI(t, 1, sleepExecutor{delay: 300 * time.Millisecond})

	first := doJSON(t, a.Routes(), http.MethodPost, "/tasks", map[string]any{"cmd": []string{"x"}})
	if first.Code != http.StatusAccepted {
		t.Fatalf("first submit status = %d, want 202", first.Code)
	}

	start := time.Now()
	second := doJSON(t, a.Routes(), http.MethodPost, "/tasks", map[string]any{"cmd": []string{"y"}})
	elapsed := time.Since(start)

	if second.Code != http.StatusAccepted {
		t.Fatalf("second submit status = %d, want 202", second.Code)
	}
	if elapsed > 50*time.Millisecond {
		t.Errorf("second Submit took %s while the only worker was busy; want ~instant (queued pending), not blocked", elapsed)
	}
}

func TestStats(t *testing.T) {
	a := newTestAPI(t, 2, sleepExecutor{delay: 200 * time.Millisecond})
	for i := 0; i < 3; i++ {
		doJSON(t, a.Routes(), http.MethodPost, "/tasks", map[string]any{"cmd": []string{"x"}})
	}
	time.Sleep(30 * time.Millisecond) // let the dispatch loop pick up the first two

	rec := doJSON(t, a.Routes(), http.MethodGet, "/stats", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	var s struct {
		Pending      int `json:"pending"`
		Running      int `json:"running"`
		Completed    int `json:"completed"`
		Failed       int `json:"failed"`
		WorkersTotal int `json:"workers_total"`
		WorkersBusy  int `json:"workers_busy"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &s); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if s.WorkersTotal != 2 {
		t.Errorf("WorkersTotal = %d, want 2", s.WorkersTotal)
	}
	if s.WorkersBusy != 2 {
		t.Errorf("WorkersBusy = %d, want 2 (2 workers, 3 tasks in flight)", s.WorkersBusy)
	}
	if s.Pending != 1 {
		t.Errorf("Pending = %d, want 1 (third task still queued)", s.Pending)
	}
}

func TestMethodNotAllowed(t *testing.T) {
	a := newTestAPI(t, 1, sleepExecutor{delay: time.Millisecond})
	rec := doJSON(t, a.Routes(), http.MethodGet, "/tasks", nil)
	if rec.Code != http.StatusMethodNotAllowed {
		t.Errorf("status = %d, want 405", rec.Code)
	}
}

func TestHealthz(t *testing.T) {
	a := newTestAPI(t, 1, sleepExecutor{delay: time.Millisecond})
	rec := doJSON(t, a.Routes(), http.MethodGet, "/healthz", nil)
	if rec.Code != http.StatusOK {
		t.Errorf("status = %d, want 200", rec.Code)
	}
}

func TestParseMemBytes(t *testing.T) {
	cases := []struct {
		in      string
		want    int64
		wantErr bool
	}{
		{"512", 512, false},
		{"512b", 512, false},
		{"1k", 1024, false},
		{"1K", 1024, false},
		{"128m", 128 * 1024 * 1024, false},
		{"1g", 1024 * 1024 * 1024, false},
		{"", 0, true},
		{"abc", 0, true},
		{"1x", 0, true},
		{"-1m", 0, true},
	}
	for _, c := range cases {
		got, err := api.ParseMemBytes(c.in)
		if c.wantErr {
			if err == nil {
				t.Errorf("ParseMemBytes(%q) = %d, nil; want error", c.in, got)
			}
			continue
		}
		if err != nil {
			t.Errorf("ParseMemBytes(%q) unexpected error: %v", c.in, err)
			continue
		}
		if got != c.want {
			t.Errorf("ParseMemBytes(%q) = %d, want %d", c.in, got, c.want)
		}
	}
}
