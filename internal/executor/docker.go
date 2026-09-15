// Package executor provides the Docker implementation of dispatch.Executor:
// it runs a task's command inside a resource-capped, disposable container and
// reports back the result.
//
// The one thing worth internalizing before reading Execute: killing our side
// of a `docker run` (the CLI process) does NOT stop the container. The
// container's lifecycle is owned by the Docker daemon (dockerd), a separate
// long-running background service; the CLI is only ever a messenger to it.
// So a task timeout — which cancels the CLI process via context — must be
// followed by an explicit, independent removal command, or the container
// leaks and keeps running forever. That's what the defer in Execute is for.
package executor

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os/exec"
	"strconv"
	"sync"
	"time"

	"github.com/roneettopiwala/relay/internal/dispatch"
	"github.com/roneettopiwala/relay/internal/task"
)

// removeTimeout bounds the cleanup "docker rm -f" call. It always runs on its
// own fresh context, never the task's ctx — that one is frequently already
// expired right when cleanup matters most (a timeout).
const removeTimeout = 5 * time.Second

// maxOutput caps how much combined stdout+stderr a task accumulates, so a
// chatty or runaway container can't grow the buffer without bound.
const maxOutput = 64 * 1024

// DockerExecutor runs tasks as Docker containers.
type DockerExecutor struct{}

// Compile-time check that DockerExecutor satisfies dispatch.Executor.
var _ dispatch.Executor = (*DockerExecutor)(nil)

// NewDockerExecutor checks that the Docker daemon is reachable and returns an
// executor. Failing fast here means a misconfigured host is caught at server
// startup, not on the first submitted task.
func NewDockerExecutor() (*DockerExecutor, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if out, err := exec.CommandContext(ctx, "docker", "info").CombinedOutput(); err != nil {
		return nil, fmt.Errorf("executor: docker daemon unreachable: %w: %s", err, bytes.TrimSpace(out))
	}
	return &DockerExecutor{}, nil
}

// Execute runs t.Spec inside a fresh container named "relay-<task ID>", with
// the requested CPU/memory limits and no network access, and reports the
// result. The container is force-removed unconditionally before Execute
// returns — on success, on failure, and on timeout/cancel alike.
func (e *DockerExecutor) Execute(ctx context.Context, t task.Task) (task.Result, error) {
	name := "relay-" + t.ID

	// Registered before cmd.Run() so it fires on every path below, including
	// a timeout that kills the docker CLI process out from under us.
	defer removeContainer(name)

	args := []string{"run", "--rm", "--name", name}
	if t.Spec.CPULimit > 0 {
		args = append(args, "--cpus", strconv.FormatFloat(t.Spec.CPULimit, 'f', -1, 64))
	}
	if t.Spec.MemLimit != "" {
		args = append(args, "--memory", t.Spec.MemLimit)
	}
	args = append(args, "--network", "none", t.Spec.Image)
	args = append(args, t.Spec.Cmd...)

	cmd := exec.CommandContext(ctx, "docker", args...)
	out := &capBuffer{max: maxOutput}
	cmd.Stdout = out
	cmd.Stderr = out

	err := cmd.Run()

	switch {
	case ctx.Err() != nil:
		// Timeout or Shutdown cancel. The CLI process is dead; the container
		// may still be running on the daemon — the deferred removeContainer
		// above is what actually cleans it up.
		return task.Result{Output: out.String()}, ctx.Err()

	case err == nil:
		return task.Result{ExitCode: 0, Output: out.String()}, nil

	default:
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			// A non-zero exit is a completed run, not an executor failure —
			// the dispatcher is what turns a non-zero ExitCode into
			// task.StatusFailed, so we just report it faithfully here.
			return task.Result{ExitCode: exitErr.ExitCode(), Output: out.String()}, nil
		}
		// Something more fundamental went wrong: docker not found, couldn't
		// start the process, etc.
		return task.Result{Output: out.String()}, fmt.Errorf("docker run: %w", err)
	}
}

// removeContainer force-removes a container by name. Errors are ignored: a
// container that never started, or was already removed by --rm on a clean
// exit, both make "docker rm -f" fail harmlessly with "no such container".
func removeContainer(name string) {
	ctx, cancel := context.WithTimeout(context.Background(), removeTimeout)
	defer cancel()
	_ = exec.CommandContext(ctx, "docker", "rm", "-f", name).Run()
}

// capBuffer is an io.Writer that keeps at most max bytes and silently drops
// anything past that, instead of growing without bound or erroring the
// command. It's used for both stdout and stderr at once, so Write must be
// safe for concurrent use — os/exec runs a separate copying goroutine per
// pipe when the destination isn't an *os.File.
type capBuffer struct {
	max int
	mu  sync.Mutex
	buf bytes.Buffer
}

func (c *capBuffer) Write(p []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if room := c.max - c.buf.Len(); room > 0 {
		if len(p) > room {
			c.buf.Write(p[:room])
		} else {
			c.buf.Write(p)
		}
	}
	// Report the full length even when we silently truncated: an io.Writer
	// that returns n < len(p) with a nil error is a contract violation and
	// would make exec.Cmd treat this as a short-write error.
	return len(p), nil
}

func (c *capBuffer) String() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.buf.String()
}
