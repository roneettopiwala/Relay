// Command relay runs the Relay HTTP server: submit a task, get an id back,
// poll status by id, backed by a Docker-executing worker pool.
package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/roneettopiwala/relay/internal/api"
	"github.com/roneettopiwala/relay/internal/dispatch"
	"github.com/roneettopiwala/relay/internal/executor"
	"github.com/roneettopiwala/relay/internal/store"
	"github.com/roneettopiwala/relay/internal/task"
)

// shutdownGrace bounds how long Shutdown waits for in-flight tasks to finish
// before forcibly cancelling them (which kills any running containers).
const shutdownGrace = 30 * time.Second

func main() {
	addr := flag.String("addr", ":8080", "HTTP listen address")
	workers := flag.Int("workers", 4, "number of concurrent workers")

	defaultImage := flag.String("default-image", "alpine", "default container image when a task omits one")
	defaultCPUs := flag.Float64("default-cpus", 0.5, "default --cpus when a task omits one")
	defaultMemory := flag.String("default-memory", "128m", "default --memory when a task omits one")
	defaultTimeout := flag.Duration("default-timeout", 30*time.Second, "default task timeout when a task omits one")

	maxCPUs := flag.Float64("max-cpus", 2.0, "maximum --cpus a task may request")
	maxMemory := flag.String("max-memory", "512m", "maximum --memory a task may request")
	maxTimeout := flag.Duration("max-timeout", 60*time.Second, "maximum task timeout a task may request")

	flag.Parse()

	maxMemBytes, err := api.ParseMemBytes(*maxMemory)
	if err != nil {
		log.Fatalf("invalid -max-memory: %v", err)
	}

	st := store.New()

	exec, err := executor.NewDockerExecutor()
	if err != nil {
		log.Fatalf("docker executor: %v (is the Docker daemon running and reachable without sudo?)", err)
	}

	d := dispatch.New(st, *workers, exec)

	defaults := task.Defaults{
		Image:    *defaultImage,
		CPULimit: *defaultCPUs,
		MemLimit: *defaultMemory,
		Timeout:  *defaultTimeout,
	}

	a := api.New(st, d, defaults, *maxCPUs, maxMemBytes, *maxTimeout)

	srv := &http.Server{
		Addr:              *addr,
		Handler:           a.Routes(),
		ReadHeaderTimeout: 5 * time.Second,
	}

	serveErr := make(chan error, 1)
	go func() {
		log.Printf("relay listening on %s with %d workers", *addr, *workers)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			serveErr <- err
		}
	}()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	select {
	case err := <-serveErr:
		log.Fatalf("listen: %v", err)
	case <-ctx.Done():
	}
	stop() // restore default signal behaviour; a second Ctrl-C now kills us immediately

	log.Println("shutting down: no longer accepting new tasks, draining in-flight work...")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownGrace)
	defer cancel()

	// Stop accepting HTTP connections first, so no new Submit races the
	// dispatcher's own shutdown below.
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Printf("http shutdown: %v", err)
	}
	if err := d.Shutdown(shutdownCtx); err != nil {
		log.Printf("dispatcher shutdown: %v (in-flight containers were force-cancelled)", err)
	}
	log.Println("shutdown complete")
}
