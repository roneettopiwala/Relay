// Command mcpserver is an MCP server exposing one tool, run_task, that
// forwards an agent's tool-call to a running Relay server instead of
// executing it locally: submit a task, wait for it to finish, and hand the
// result back as the tool's response. Runs over stdio, the transport an MCP
// client like Claude Code uses to launch and talk to a local server.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/roneettopiwala/relay/internal/relayclient"
	"github.com/roneettopiwala/relay/internal/task"
)

// runTaskInput is the run_task tool's argument schema. The go-sdk generates
// the JSON schema Claude Code sees from these json/jsonschema struct tags —
// same pattern internal/api's submitRequest uses for its own HTTP body.
type runTaskInput struct {
	Image          string   `json:"image,omitempty" jsonschema:"container image to run the command in; defaults to the Relay server's configured default image"`
	Cmd            []string `json:"cmd" jsonschema:"the command and its arguments to run inside the container, e.g. [\"ls\",\"-la\"]"`
	CPUs           *float64 `json:"cpus,omitempty" jsonschema:"CPU limit for the container, e.g. 0.5; defaults to the server's configured default"`
	Memory         string   `json:"memory,omitempty" jsonschema:"memory limit for the container, e.g. 256m; defaults to the server's configured default"`
	TimeoutSeconds *int     `json:"timeout_seconds,omitempty" jsonschema:"maximum seconds the task may run before it is killed; defaults to the server's configured default"`
}

func main() {
	relayURL := flag.String("relay-url", "http://localhost:8080", "Relay server base URL")
	pollInterval := flag.Duration("poll-interval", 100*time.Millisecond, "how often to poll a submitted task's status")
	extraWait := flag.Duration("extra-wait", 10*time.Second, "margin added on top of a task's own timeout before this tool gives up waiting for it")
	// maxWait exists because this server has no visibility into Relay's own
	// retry policy (max attempts, backoff) — a single-attempt margin isn't
	// enough once retries are in play. Found live: a 2s task timeout with
	// Relay's default 3 retries took ~12.3s to reach a final result, just
	// over a naive task_timeout+extraWait budget. Rather than try to
	// reverse-engineer Relay's retry timing from here, wait generously,
	// bounded only against a genuinely stuck server.
	maxWait := flag.Duration("max-wait", 5*time.Minute, "hard ceiling on how long this tool waits for one task, covering Relay's own retries/backoff on top of the task's own timeout")
	flag.Parse()

	rc := relayclient.New(*relayURL, nil)

	server := mcp.NewServer(&mcp.Implementation{Name: "relay", Version: "0.1.0"}, nil)
	mcp.AddTool(server, &mcp.Tool{
		Name: "run_task",
		Description: "Run a command in a resource-capped, disposable Docker container via the Relay " +
			"task scheduler, and return its output. Blocks until the task finishes, fails, or times out.",
	}, makeRunTaskHandler(rc, *pollInterval, *extraWait, *maxWait))

	if err := server.Run(context.Background(), &mcp.StdioTransport{}); err != nil {
		log.Fatalf("mcpserver: %v", err)
	}
}

// makeRunTaskHandler closes over the Relay client and timing config so the
// handler itself (the part the SDK actually calls) stays a plain function of
// (ctx, request, input) — the shape mcp.AddTool requires.
func makeRunTaskHandler(rc *relayclient.Client, pollInterval, extraWait, maxWait time.Duration) func(context.Context, *mcp.CallToolRequest, runTaskInput) (*mcp.CallToolResult, any, error) {
	return func(ctx context.Context, req *mcp.CallToolRequest, in runTaskInput) (*mcp.CallToolResult, any, error) {
		// Verified against the SDK's actual AddTool wrapper (mcp/server.go,
		// toolForErr): a plain Go error returned from this handler is caught
		// and converted into CallToolResult{IsError: true} with the error
		// text as Content — it is NOT surfaced as a client-visible err from
		// CallTool (that only happens for a *jsonrpc.Error, which nothing
		// here returns). So every path below — a malformed call, a failed
		// submission, a poll that gives up, and a task that itself failed —
		// all end up as an IsError result the agent can read and react to;
		// none of them are protocol-level failures. That's deliberate and
		// matches the SDK's own guidance: only "the tool doesn't exist" /
		// server-level problems belong at the protocol-error level, not a
		// specific tool invocation not working out.
		if len(in.Cmd) == 0 {
			return nil, nil, fmt.Errorf("cmd must be a non-empty array")
		}

		submitted, err := rc.Submit(ctx, relayclient.SubmitRequest{
			Image: in.Image, Cmd: in.Cmd, CPUs: in.CPUs, Memory: in.Memory, TimeoutSeconds: in.TimeoutSeconds,
		})
		if err != nil {
			return nil, nil, fmt.Errorf("submit task: %w", err)
		}

		// Wait at least a bit longer than the task's own configured timeout
		// (so Relay's own timeout has a chance to fire and produce a clean
		// result), but never less than maxWait — Relay may retry a failed
		// attempt several times with backoff before giving up for good, and
		// this server has no visibility into that policy to compute an exact
		// budget for it. A fast task still returns fast either way; this only
		// changes how long we're willing to wait for a slow/retrying one.
		timeout := extraWait
		if submitted.Spec.Timeout > 0 {
			timeout = submitted.Spec.Timeout + extraWait
		}
		if timeout < maxWait {
			timeout = maxWait
		}

		final, err := rc.WaitForTerminal(ctx, submitted.ID, pollInterval, timeout)
		if err != nil {
			return nil, nil, fmt.Errorf("wait for task %s: %w", submitted.ID, err)
		}

		// Here there IS a full task outcome (not just an error string), so we
		// build the result explicitly instead of returning a plain error —
		// this is what puts the exit code and captured output in front of
		// the agent, not just the one-line failure reason.
		return &mcp.CallToolResult{
			Content: []mcp.Content{&mcp.TextContent{Text: formatResult(final)}},
			IsError: final.Status == task.StatusFailed,
		}, nil, nil
	}
}

func formatResult(t task.Task) string {
	var b strings.Builder
	fmt.Fprintf(&b, "status: %s\n", t.Status)
	if t.ExitCode != nil {
		fmt.Fprintf(&b, "exit code: %d\n", *t.ExitCode)
	}
	if t.Error != "" {
		fmt.Fprintf(&b, "error: %s\n", t.Error)
	}
	if t.Output != "" {
		fmt.Fprintf(&b, "output:\n%s", t.Output)
	}
	return b.String()
}
