package security

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

func TestSaveIdentityStoreSetsFileMode0640(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("unix file mode semantics do not apply on windows")
	}

	path := filepath.Join(t.TempDir(), "security", "auth.json")
	if err := saveIdentityStore(path, defaultIdentityStore()); err != nil {
		t.Fatalf("saveIdentityStore failed: %v", err)
	}

	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat auth store failed: %v", err)
	}
	if got := info.Mode().Perm(); got != 0o640 {
		t.Fatalf("expected auth store mode 0640, got %o", got)
	}
}
