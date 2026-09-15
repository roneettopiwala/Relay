package executor

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/roneettopiwala/relay/internal/task"
)

// requireDocker skips the test if docker isn't available, so this file
// degrades gracefully on a machine without Docker instead of failing.
func requireDocker(t *testing.T) {
	t.Helper()
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker not found in PATH")
	}
	if err := exec.Command("docker", "info").Run(); err != nil {
		t.Skip("docker daemon not reachable")
	}
}

func newTestExecutor(t *testing.T) *DockerExecutor {
	t.Helper()
	requireDocker(t)
	e, err := NewDockerExecutor()
	if err != nil {
		t.Fatalf("NewDockerExecutor: %v", err)
	}
	return e
}

func randID(t *testing.T) string {
	t.Helper()
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		t.Fatalf("rand.Read: %v", err)
	}
	return hex.EncodeToString(b[:])
}

// containerExists checks the Docker daemon directly (not our own bookkeeping)
// for a container with an exact name match.
func containerExists(t *testing.T, name string) bool {
	t.Helper()
	out, err := exec.Command("docker", "ps", "-a",
		"--filter", "name=^/"+name+"$", "--format", "{{.Names}}").Output()
	if err != nil {
		t.Fatalf("docker ps: %v", err)
	}
	return strings.TrimSpace(string(out)) != ""
}

func TestExecute_Success(t *testing.T) {
	e := newTestExecutor(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	tk := task.Task{ID: randID(t), Spec: task.Spec{
		Image: "alpine", Cmd: []string{"echo", "hello-relay"},
		CPULimit: 1.0, MemLimit: "64m",
	}}

	result, err := e.Execute(ctx, tk)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if result.ExitCode != 0 {
		t.Errorf("ExitCode = %d, want 0", result.ExitCode)
	}
	if !strings.Contains(result.Output, "hello-relay") {
		t.Errorf("Output = %q, want it to contain %q", result.Output, "hello-relay")
	}
	if containerExists(t, "relay-"+tk.ID) {
		t.Errorf("container relay-%s still exists after a clean exit", tk.ID)
	}
}

func TestExecute_NonZeroExit(t *testing.T) {
	e := newTestExecutor(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	tk := task.Task{ID: randID(t), Spec: task.Spec{
		Image: "alpine", Cmd: []string{"sh", "-c", "exit 7"},
		CPULimit: 1.0, MemLimit: "64m",
	}}

	result, err := e.Execute(ctx, tk)
	if err != nil {
		t.Fatalf("Execute: %v (a non-zero exit should be a Result, not an error)", err)
	}
	if result.ExitCode != 7 {
		t.Errorf("ExitCode = %d, want 7", result.ExitCode)
	}
}

// TestExecute_TimeoutKillsAndCleansUp is the main Checkpoint 3 break-it test:
// a task that outlives its timeout must be cut off around the deadline (not
// immediately, not left running the full 30s), and must not leak a container.
func TestExecute_TimeoutKillsAndCleansUp(t *testing.T) {
	e := newTestExecutor(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	tk := task.Task{ID: randID(t), Spec: task.Spec{
		Image: "alpine", Cmd: []string{"sh", "-c", "sleep 30"},
		CPULimit: 1.0, MemLimit: "64m",
	}}

	start := time.Now()
	_, err := e.Execute(ctx, tk)
	elapsed := time.Since(start)

	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want context.DeadlineExceeded", err)
	}
	if elapsed < 1500*time.Millisecond || elapsed > 8*time.Second {
		t.Errorf("timeout fired after %s, want ~2s (not immediate, not left running toward 30s)", elapsed)
	}
	if containerExists(t, "relay-"+tk.ID) {
		t.Errorf("container relay-%s leaked after timeout", tk.ID)
	}
}

// TestExecute_ConcurrentTimeoutsCleanUp runs several timing-out tasks at once
// to make sure cleanup isn't only correct in the single-task case.
func TestExecute_ConcurrentTimeoutsCleanUp(t *testing.T) {
	e := newTestExecutor(t)
	const n = 3
	ids := make([]string, n)
	var wg sync.WaitGroup

	for i := 0; i < n; i++ {
		ids[i] = randID(t)
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			tk := task.Task{ID: id, Spec: task.Spec{
				Image: "alpine", Cmd: []string{"sh", "-c", "sleep 30"},
				CPULimit: 1.0, MemLimit: "64m",
			}}
			if _, err := e.Execute(ctx, tk); !errors.Is(err, context.DeadlineExceeded) {
				t.Errorf("task %s: err = %v, want DeadlineExceeded", id, err)
			}
		}(ids[i])
	}
	wg.Wait()

	for _, id := range ids {
		if containerExists(t, "relay-"+id) {
			t.Errorf("container relay-%s leaked after concurrent timeout", id)
		}
	}
}

// TestExecute_MemoryLimitOOMKill checks --memory is a real, enforced limit:
// a container that grows past it is OOM-killed by the kernel cgroup (exit
// 137), not merely slowed down or allowed to keep growing.
func TestExecute_MemoryLimitOOMKill(t *testing.T) {
	e := newTestExecutor(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	tk := task.Task{ID: randID(t), Spec: task.Spec{
		Image: "alpine",
		// Doubles a shell variable's length until the kernel OOM-kills the
		// container; needs no extra tools, so it works in bare alpine.
		Cmd:      []string{"sh", "-c", `x="a"; while true; do x="$x$x"; done`},
		CPULimit: 1.0, MemLimit: "16m",
	}}

	result, err := e.Execute(ctx, tk)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if result.ExitCode != 137 {
		t.Errorf("ExitCode = %d, want 137 (SIGKILL from the cgroup OOM killer); output=%q",
			result.ExitCode, result.Output)
	}
	if containerExists(t, "relay-"+tk.ID) {
		t.Errorf("container relay-%s still exists after OOM kill", tk.ID)
	}
}

// TestCapBuffer_Truncates is a pure unit test (no Docker) for the output cap.
func TestCapBuffer_Truncates(t *testing.T) {
	c := &capBuffer{max: 10}

	n, err := c.Write([]byte("0123456789ABCDEF")) // 16 bytes, 6 over the cap
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	if n != 16 {
		t.Errorf("Write returned n=%d, want 16 (must report full length even when truncating internally)", n)
	}
	if got := c.String(); got != "0123456789" {
		t.Errorf("String() = %q, want %q", got, "0123456789")
	}

	// Further writes past max are dropped entirely but still "succeed" from
	// the writer's point of view.
	n2, err := c.Write([]byte("more"))
	if err != nil || n2 != 4 {
		t.Errorf("second Write = (%d, %v), want (4, nil)", n2, err)
	}
	if got := c.String(); got != "0123456789" {
		t.Errorf("String() after overflow write = %q, want unchanged %q", got, "0123456789")
	}
}
