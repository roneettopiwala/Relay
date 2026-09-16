package main

import (
	"context"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/roneettopiwala/relay/internal/dispatch"
	"github.com/roneettopiwala/relay/internal/task"
	"github.com/roneettopiwala/relay/internal/testutil"
)

// failExecutor always returns a non-zero exit code — a permanent task
// failure, to test the IsError path.
type failExecutor struct{}

func (failExecutor) Execute(ctx context.Context, t task.Task) (task.Result, error) {
	return task.Result{ExitCode: 1}, nil
}

// connectToServer launches the mcpserver binary as a real subprocess (via
// `go run .`, same as an MCP client actually would) pointed at relayURL, and
// connects an MCP client to it over stdio — exactly the path Claude Code
// itself takes, just driven by the SDK's own client instead of Claude Code.
func connectToServer(t *testing.T, ctx context.Context, relayURL string, extraArgs ...string) *mcp.ClientSession {
	t.Helper()
	args := append([]string{"run", ".", "-relay-url", relayURL, "-poll-interval", "10ms"}, extraArgs...)
	cmd := exec.Command("go", args...)
	transport := &mcp.CommandTransport{Command: cmd}
	client := mcp.NewClient(&mcp.Implementation{Name: "test-client", Version: "v0.0.0"}, nil)

	cs, err := client.Connect(ctx, transport, nil)
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	t.Cleanup(func() { cs.Close() })
	return cs
}

func contentText(t *testing.T, result *mcp.CallToolResult) string {
	t.Helper()
	if len(result.Content) == 0 {
		t.Fatal("result has no content")
	}
	tc, ok := result.Content[0].(*mcp.TextContent)
	if !ok {
		t.Fatalf("content[0] is %T, want *mcp.TextContent", result.Content[0])
	}
	return tc.Text
}

// TestRunTask_Success is the Checkpoint 2 break-it test: a real MCP client,
// talking to a real subprocess over stdio, calling run_task and getting back
// a real completed result — the whole Phase 3 chain except Claude Code
// itself.
func TestRunTask_Success(t *testing.T) {
	srv := testutil.NewAPIServer(t, testutil.SleepExecutor{Delay: 10 * time.Millisecond}, nil, testutil.DefaultTestLimits)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cs := connectToServer(t, ctx, srv.URL)

	result, err := cs.CallTool(ctx, &mcp.CallToolParams{
		Name:      "run_task",
		Arguments: map[string]any{"image": "alpine", "cmd": []string{"echo", "hi"}},
	})
	if err != nil {
		t.Fatalf("CallTool: %v", err)
	}
	if result.IsError {
		t.Fatalf("IsError = true, want false; content=%q", contentText(t, result))
	}
	if text := contentText(t, result); !strings.Contains(text, "status: completed") {
		t.Errorf("result text = %q, want it to mention status: completed", text)
	}
}

// TestRunTask_PermanentFailureReportsIsError checks a failed task comes back
// as IsError=true with the failure detail in Content — per the SDK's own
// guidance, this is what lets an agent actually see and react to the
// failure, versus an opaque Go-error protocol failure.
func TestRunTask_PermanentFailureReportsIsError(t *testing.T) {
	srv := testutil.NewAPIServer(t, failExecutor{}, nil, testutil.DefaultTestLimits)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cs := connectToServer(t, ctx, srv.URL)

	result, err := cs.CallTool(ctx, &mcp.CallToolParams{
		Name:      "run_task",
		Arguments: map[string]any{"image": "alpine", "cmd": []string{"x"}},
	})
	if err != nil {
		t.Fatalf("CallTool: %v", err)
	}
	if !result.IsError {
		t.Errorf("IsError = false, want true for a permanently-failed task")
	}
	if text := contentText(t, result); !strings.Contains(text, "status: failed") {
		t.Errorf("result text = %q, want it to mention status: failed", text)
	}
}

// TestRunTask_MissingCmdIsToolError checks a malformed call (no cmd) comes
// back as IsError=true, not a fabricated success. Confirmed against the
// SDK's actual AddTool wrapper (see the comment above the handler in
// main.go): a plain Go error return never surfaces as a client-visible
// CallTool error, only as an IsError result — so that, not err != nil, is
// the thing to assert here.
func TestRunTask_MissingCmdIsToolError(t *testing.T) {
	srv := testutil.NewAPIServer(t, testutil.SleepExecutor{Delay: time.Millisecond}, nil, testutil.DefaultTestLimits)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cs := connectToServer(t, ctx, srv.URL)

	result, err := cs.CallTool(ctx, &mcp.CallToolParams{
		Name:      "run_task",
		Arguments: map[string]any{"image": "alpine"}, // no cmd
	})
	if err != nil {
		t.Fatalf("CallTool: %v (want a nil error with IsError=true instead)", err)
	}
	if !result.IsError {
		t.Errorf("IsError = false, want true for a call with no cmd")
	}
	if text := contentText(t, result); text == "" {
		t.Error("result text is empty, want a reason for the failure")
	}
}

// TestRunTask_RelayUnreachableIsToolError checks that if Relay itself can't
// be reached, the tool call comes back as IsError=true with a descriptive
// reason — same SDK behaviour as the missing-cmd case above.
func TestRunTask_RelayUnreachableIsToolError(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	// Port 1 is never going to have anything listening on it.
	cs := connectToServer(t, ctx, "http://127.0.0.1:1")

	result, err := cs.CallTool(ctx, &mcp.CallToolParams{
		Name:      "run_task",
		Arguments: map[string]any{"image": "alpine", "cmd": []string{"echo", "hi"}},
	})
	if err != nil {
		t.Fatalf("CallTool: %v (want a nil error with IsError=true instead)", err)
	}
	if !result.IsError {
		t.Errorf("IsError = false, want true when Relay is unreachable")
	}
	if text := contentText(t, result); !strings.Contains(text, "submit task") {
		t.Errorf("result text = %q, want it to mention the submit failure", text)
	}
}

// alwaysTimeoutExecutor blocks until ctx is done on every call — used to
// force a task to hit its own timeout on every attempt, exhausting retries
// rather than ever succeeding.
type alwaysTimeoutExecutor struct{}

func (alwaysTimeoutExecutor) Execute(ctx context.Context, t task.Task) (task.Result, error) {
	<-ctx.Done()
	return task.Result{}, ctx.Err()
}

// TestRunTask_WaitsPastExtraWaitForRetries is a regression test for a real
// bug found by hand: a task's own timeout plus extraWait isn't enough
// budget once Relay's retry policy is in play. With MaxRetries=2 and a 1s
// per-attempt timeout, 3 attempts plus backoff between them takes noticeably
// longer than a single-cycle margin allows — this pins that -max-wait acts
// as a floor under that budget, so the tool waits for Relay's real final
// answer instead of giving up early.
func TestRunTask_WaitsPastExtraWaitForRetries(t *testing.T) {
	srv := testutil.NewAPIServerWithConfig(t, alwaysTimeoutExecutor{}, nil, testutil.DefaultTestLimits, dispatch.Config{
		Workers: 1, MaxRetries: 2, RetryBaseDelay: 50 * time.Millisecond, RetryMaxDelay: 100 * time.Millisecond,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	// extra-wait is deliberately far too small to cover 3 attempts (~3s) on
	// its own — max-wait is what has to rescue this call.
	cs := connectToServer(t, ctx, srv.URL, "-extra-wait", "50ms", "-max-wait", "5s")

	timeoutSecs := 1
	result, err := cs.CallTool(ctx, &mcp.CallToolParams{
		Name: "run_task",
		Arguments: map[string]any{
			"image": "alpine", "cmd": []string{"x"}, "timeout_seconds": timeoutSecs,
		},
	})
	if err != nil {
		t.Fatalf("CallTool: %v (the -max-wait floor should have covered the retries)", err)
	}
	if !result.IsError {
		t.Errorf("IsError = false, want true (task exhausts retries and fails)")
	}
	if text := contentText(t, result); !strings.Contains(text, "gave up after") {
		t.Errorf("result text = %q, want it to mention giving up after retries", text)
	}
}
