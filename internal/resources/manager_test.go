package resources

import (
	"context"
	"testing"
)

func TestResourceWaitsDoNotThrottle(t *testing.T) {
	m := NewManager(DefaultConfig())
	ctx := context.Background()

	if err := m.WaitInsertAllowance(ctx); err != nil {
		t.Fatalf("wait insert allowance: %v", err)
	}
	if err := m.WaitIndexingAllowance(ctx); err != nil {
		t.Fatalf("wait indexing allowance: %v", err)
	}
	if err := m.WaitForMemory(ctx, 1024, "test"); err != nil {
		t.Fatalf("wait for memory: %v", err)
	}
}
