package relayclient_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/roneettopiwala/relay/internal/dispatch"
	"github.com/roneettopiwala/relay/internal/relayclient"
	"github.com/roneettopiwala/relay/internal/task"
	"github.com/roneettopiwala/relay/internal/testutil"
)

// newTestServer spins up the real internal/api handler (not a mock) behind a
// real httptest.Server, so relayclient is tested against genuine HTTP calls
// to Relay's actual API package.
func newTestServer(t *testing.T, exec dispatch.Executor) *httptest.Server {
	t.Helper()
	return testutil.NewAPIServer(t, exec, nil, testutil.DefaultTestLimits)
}

func TestSubmitAndGet(t *testing.T) {
	srv := newTestServer(t, testutil.SleepExecutor{Delay: 10 * time.Millisecond})
	c := relayclient.New(srv.URL, nil)

	submitted, err := c.Submit(context.Background(), relayclient.SubmitRequest{
		Image: "alpine", Cmd: []string{"echo", "hi"},
	})
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if submitted.ID == "" {
		t.Fatal("Submit returned an empty id")
	}
	if submitted.Status != task.StatusPending {
		t.Errorf("status = %s, want pending", submitted.Status)
	}

	got, err := c.Get(context.Background(), submitted.ID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.ID != submitted.ID {
		t.Errorf("Get returned id %q, want %q", got.ID, submitted.ID)
	}
}

func TestSubmitInvalid(t *testing.T) {
	srv := newTestServer(t, testutil.SleepExecutor{Delay: time.Millisecond})
	c := relayclient.New(srv.URL, nil)

	_, err := c.Submit(context.Background(), relayclient.SubmitRequest{Image: "alpine"}) // no Cmd
	if err == nil {
		t.Fatal("Submit succeeded with no cmd, want an error")
	}
	var apiErr *relayclient.APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("err = %v (%T), want *relayclient.APIError", err, err)
	}
	if apiErr.StatusCode != http.StatusBadRequest {
		t.Errorf("StatusCode = %d, want 400", apiErr.StatusCode)
	}
}

func TestGetNotFound(t *testing.T) {
	srv := newTestServer(t, testutil.SleepExecutor{Delay: time.Millisecond})
	c := relayclient.New(srv.URL, nil)

	_, err := c.Get(context.Background(), "no-such-id")
	var apiErr *relayclient.APIError
	if !errors.As(err, &apiErr) || apiErr.StatusCode != http.StatusNotFound {
		t.Fatalf("err = %v, want *APIError{StatusCode: 404}", err)
	}
}

func TestWaitForTerminal_Success(t *testing.T) {
	srv := newTestServer(t, testutil.SleepExecutor{Delay: 30 * time.Millisecond})
	c := relayclient.New(srv.URL, nil)

	submitted, err := c.Submit(context.Background(), relayclient.SubmitRequest{Image: "alpine", Cmd: []string{"x"}})
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}

	got, err := c.WaitForTerminal(context.Background(), submitted.ID, 5*time.Millisecond, time.Second)
	if err != nil {
		t.Fatalf("WaitForTerminal: %v", err)
	}
	if got.Status != task.StatusCompleted {
		t.Errorf("status = %s, want completed", got.Status)
	}
}

func TestWaitForTerminal_Timeout(t *testing.T) {
	srv := newTestServer(t, testutil.SleepExecutor{Delay: time.Hour}) // won't finish within this test
	c := relayclient.New(srv.URL, nil)

	// Give the task itself a short server-side timeout too — otherwise the
	// dispatcher's own graceful Shutdown (in this test's cleanup) has to wait
	// out the *server's default* 30s task timeout before it can exit, since
	// nothing else would ever make the (fake, time.Hour-long) executor return.
	shortTimeout := 1
	submitted, err := c.Submit(context.Background(), relayclient.SubmitRequest{
		Image: "alpine", Cmd: []string{"x"}, TimeoutSeconds: &shortTimeout,
	})
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}

	_, err = c.WaitForTerminal(context.Background(), submitted.ID, 5*time.Millisecond, 50*time.Millisecond)
	if !errors.Is(err, relayclient.ErrPollTimeout) {
		t.Errorf("err = %v, want ErrPollTimeout", err)
	}
}
