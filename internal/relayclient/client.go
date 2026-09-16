// Package relayclient is a small typed HTTP client for Relay's own API:
// submit a task, fetch it by id, or wait for it to reach a terminal state.
// cmd/loadtest and the Phase 3 MCP server both need the identical HTTP
// calls, so this is the one implementation, not two.
package relayclient

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/roneettopiwala/relay/internal/task"
)

// ErrPollTimeout is returned by WaitForTerminal when pollTimeout elapses
// before the task reaches a terminal state — distinct from a request itself
// failing, so a caller can tell "we gave up waiting" from "something broke".
var ErrPollTimeout = errors.New("relayclient: timed out waiting for a terminal state")

// APIError is returned when the server responds with a non-2xx status.
// Exposing StatusCode lets a caller branch on something specific (e.g. 503
// backpressure) without parsing the error string.
type APIError struct {
	StatusCode int
	Body       string
}

func (e *APIError) Error() string {
	return fmt.Sprintf("relayclient: status %d: %s", e.StatusCode, e.Body)
}

// SubmitRequest mirrors internal/api's (unexported) submitRequest wire type.
// Kept as a separate type here rather than imported — that struct is an HTTP
// wire format internal to the api package, not meant to be a shared type.
type SubmitRequest struct {
	Image          string   `json:"image"`
	Cmd            []string `json:"cmd"`
	CPUs           *float64 `json:"cpus,omitempty"`
	Memory         string   `json:"memory,omitempty"`
	TimeoutSeconds *int     `json:"timeout_seconds,omitempty"`
}

// Client talks to a running Relay server's HTTP API.
type Client struct {
	baseURL string
	http    *http.Client
}

// New builds a Client for the server at baseURL (e.g. "http://localhost:8080").
// httpClient may be nil to get a sensible default; pass your own (e.g. with a
// tuned Transport) for a high-concurrency caller like a load test.
func New(baseURL string, httpClient *http.Client) *Client {
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 15 * time.Second}
	}
	return &Client{baseURL: baseURL, http: httpClient}
}

// Submit posts a new task and returns it (status "pending").
func (c *Client) Submit(ctx context.Context, req SubmitRequest) (task.Task, error) {
	body, err := json.Marshal(req)
	if err != nil {
		return task.Task{}, fmt.Errorf("relayclient: marshal request: %w", err)
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/tasks", bytes.NewReader(body))
	if err != nil {
		return task.Task{}, fmt.Errorf("relayclient: build request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	return c.do(httpReq, http.StatusAccepted)
}

// Get fetches a task's current state by id.
func (c *Client) Get(ctx context.Context, id string) (task.Task, error) {
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/tasks/"+id, nil)
	if err != nil {
		return task.Task{}, fmt.Errorf("relayclient: build request: %w", err)
	}
	return c.do(httpReq, http.StatusOK)
}

func (c *Client) do(httpReq *http.Request, wantStatus int) (task.Task, error) {
	resp, err := c.http.Do(httpReq)
	if err != nil {
		return task.Task{}, err
	}
	defer resp.Body.Close()

	data, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != wantStatus {
		return task.Task{}, &APIError{StatusCode: resp.StatusCode, Body: string(data)}
	}

	var t task.Task
	if err := json.Unmarshal(data, &t); err != nil {
		return task.Task{}, fmt.Errorf("relayclient: decode response: %w", err)
	}
	return t, nil
}

// WaitForTerminal polls Get every pollInterval until the task reaches a
// terminal state, ctx is done, or pollTimeout elapses (ErrPollTimeout) —
// whichever comes first.
func (c *Client) WaitForTerminal(ctx context.Context, id string, pollInterval, pollTimeout time.Duration) (task.Task, error) {
	deadline := time.Now().Add(pollTimeout)
	for {
		t, err := c.Get(ctx, id)
		if err != nil {
			return task.Task{}, err
		}
		if t.Terminal() {
			return t, nil
		}
		if time.Now().After(deadline) {
			return task.Task{}, ErrPollTimeout
		}
		select {
		case <-time.After(pollInterval):
		case <-ctx.Done():
			return task.Task{}, ctx.Err()
		}
	}
}
