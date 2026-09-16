package metrics_test

import (
	"sync"
	"testing"
	"time"

	"github.com/roneettopiwala/relay/internal/metrics"
)

func TestSnapshot_Empty(t *testing.T) {
	r := metrics.New()
	s := r.Snapshot()
	if s.SampleCount != 0 || s.P50 != 0 || s.P99 != 0 {
		t.Errorf("Snapshot on empty recorder = %+v, want all zero", s)
	}
}

func TestSnapshot_BasicPercentiles(t *testing.T) {
	r := metrics.New()
	// 1ms, 2ms, ..., 100ms — nearest-rank P50 of 100 sorted values is index
	// int(0.5*99)=49 -> the 50th value (1-indexed) = 50ms; P99 is index
	// int(0.99*99)=98 -> the 99th value = 99ms.
	for i := 1; i <= 100; i++ {
		r.Record(time.Duration(i) * time.Millisecond)
	}
	s := r.Snapshot()
	if s.SampleCount != 100 {
		t.Errorf("SampleCount = %d, want 100", s.SampleCount)
	}
	if s.P50 != 50*time.Millisecond {
		t.Errorf("P50 = %s, want 50ms", s.P50)
	}
	if s.P99 != 99*time.Millisecond {
		t.Errorf("P99 = %s, want 99ms", s.P99)
	}
}

func TestSnapshot_WindowWraps(t *testing.T) {
	r := metrics.New()
	// Record 250 entries into a 200-capacity window: values 1..50 should be
	// evicted, leaving 51..250 (200 entries).
	for i := 1; i <= 250; i++ {
		r.Record(time.Duration(i) * time.Millisecond)
	}
	s := r.Snapshot()
	if s.SampleCount != 200 {
		t.Fatalf("SampleCount = %d, want 200 (window capacity)", s.SampleCount)
	}
	// Remaining values are 51..250 (200 values); P50 (nearest-rank, index 99
	// of 200) = the 100th smallest = 150ms.
	if s.P50 != 150*time.Millisecond {
		t.Errorf("P50 = %s, want 150ms (oldest entries should have been evicted)", s.P50)
	}
}

func TestRecorder_ConcurrentSafe(t *testing.T) {
	r := metrics.New()
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 20; j++ {
				r.Record(time.Duration(i*20+j) * time.Millisecond)
				_ = r.Snapshot()
			}
		}(i)
	}
	wg.Wait()
	// Just needs to survive -race and produce a sane final snapshot.
	s := r.Snapshot()
	if s.SampleCount != 200 {
		t.Errorf("SampleCount = %d, want 200 (1000 records into a 200 window)", s.SampleCount)
	}
}
