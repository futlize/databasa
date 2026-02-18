package resources

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func TestWaitInsertAllowanceCancellationWhenThrottled(t *testing.T) {
	m := &Manager{
		memoryBudgetBytes:  100,
		throttleEnterBytes: 80,
		throttleExitBytes:  70,
	}
	atomic.StoreUint64(&m.sampledMemoryUsageBytes, 90)
	atomic.StoreUint32(&m.insertThrottlingActive, 1)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Millisecond)
	defer cancel()

	start := time.Now()
	err := m.WaitInsertAllowance(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected deadline exceeded, got %v", err)
	}
	if elapsed := time.Since(start); elapsed > 500*time.Millisecond {
		t.Fatalf("wait did not return promptly under cancellation, elapsed=%s", elapsed)
	}
}
