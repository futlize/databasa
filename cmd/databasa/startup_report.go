package main

import (
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/futlize/databasa/internal/server"
	"github.com/futlize/databasa/internal/storage"
	sysmemory "github.com/pbnjay/memory"
)

type startupField struct {
	name  string
	value string
}

func logStartupInitializationReport(cfg AppConfig, configPath string, opts storage.Options, walMode storage.WALSyncMode, srv *server.Server, listenAddr string) {
	dataDir := absoluteOrOriginal(cfg.Storage.DataDir)
	walDir := filepath.Join(dataDir, "<collection>", "shards", "<shard-id>", "wal")

	totalMem := sysmemory.TotalMemory()
	freeMem := sysmemory.FreeMemory()
	freeDisk, diskErr := diskFreeBytes(dataDir)

	rec := srv.StartupRecoveryReport()
	kernel := strings.TrimSpace(kernelVersion())
	if kernel == "" {
		kernel = "unknown"
	}

	log.Printf("INFO startup initialization report")

	logInfoSection("System", []startupField{
		{name: "os name", value: runtime.GOOS},
		{name: "kernel version", value: kernel},
		{name: "go version", value: runtime.Version()},
		{name: "cpu architecture", value: runtime.GOARCH},
		{name: "detected cpu cores", value: formatCount(int64(runtime.NumCPU()))},
		{name: "gomaxprocs", value: formatCount(int64(runtime.GOMAXPROCS(0)))},
		{name: "total system memory", value: formatBytes(totalMem)},
		{name: "available system memory", value: formatBytes(freeMem)},
	})

	storageFields := []startupField{
		{name: "database directory path", value: dataDir},
		{name: "wal directory path", value: walDir},
		{name: "wal mode", value: walModeSummary(cfg.Storage.WriteMode, walMode, opts)},
		{name: "fsync policy", value: fsyncPolicySummary(walMode, opts)},
		{name: "mmap", value: "disabled"},
		{name: "estimated page/cache size", value: formatBytes(uint64(os.Getpagesize()))},
		{name: "config file", value: absoluteOrOriginal(configPath)},
	}
	if diskErr != nil {
		storageFields = append(storageFields, startupField{name: "free disk space", value: "unavailable (" + diskErr.Error() + ")"})
	} else {
		storageFields = append(storageFields, startupField{name: "free disk space", value: formatBytes(freeDisk)})
	}
	logInfoSection("Storage", storageFields)

	backgroundWorkers := 1 + opts.IndexWorkers + opts.MergeWorkers // seal + index + merge
	logInfoSection("Engine", []startupField{
		{name: "memory budget configured", value: "unbounded (uses available system RAM)"},
		{name: "worker pool sizes", value: fmt.Sprintf("insert=unbounded search=unbounded background=%d", backgroundWorkers)},
		{name: "insert workers", value: "unbounded (request-driven)"},
		{name: "search workers", value: "unbounded (request-driven)"},
		{name: "background/optimizer workers", value: fmt.Sprintf("%d (seal=1 index=%d merge=%d)", backgroundWorkers, opts.IndexWorkers, opts.MergeWorkers)},
		{name: "compaction workers", value: formatCount(int64(opts.MergeWorkers))},
		{name: "insert queue capacity", value: formatCount(int64(opts.WALQueueSize))},
		{name: "index queue capacity", value: formatCount(int64(opts.OptimizerQueueCap))},
		{name: "optimizer queue capacity", value: formatCount(int64(opts.OptimizerQueueCap))},
		{name: "listen address", value: listenAddr},
	})

	logInfoSection("Index", []startupField{
		{name: "ann index type", value: "hnsw"},
		{name: "hnsw parameters", value: fmt.Sprintf("M=%d efConstruction=%d efSearch=%d", opts.HNSWConfig.M, opts.HNSWConfig.EfConstruction, opts.HNSWConfig.EfSearch)},
		{name: "index persistence mode", value: "partition-scoped on-disk + in-memory serving"},
	})

	logInfoSection("Recovery", []startupField{
		{name: "recovery needed", value: yesNo(rec.RecoveryNeeded)},
		{name: "wal replay duration", value: formatDuration(rec.WALReplayDuration)},
		{name: "recovered operations", value: formatCount(int64(rec.RecoveredOperations))},
		{name: "partitions loaded", value: formatCount(int64(rec.LoadedPartitions))},
		{name: "shards loaded", value: formatCount(int64(rec.LoadedShards))},
		{name: "last checkpoint timestamp", value: formatTime(rec.LastCheckpoint)},
	})
}

func logInfoSection(section string, fields []startupField) {
	log.Printf("INFO [%s]", section)
	for _, field := range fields {
		log.Printf("INFO   %-28s %s", field.name+":", field.value)
	}
}

func fatalStartup(stage string, err error) {
	log.Printf("ERROR startup failed at %s: %v", stage, err)

	var pathErr *os.PathError
	if errors.As(err, &pathErr) {
		log.Printf("ERROR diagnostic: filesystem operation failed op=%s path=%s", pathErr.Op, pathErr.Path)
	}

	msg := strings.ToLower(err.Error())
	switch {
	case errors.Is(err, os.ErrPermission) || strings.Contains(msg, "permission denied"):
		log.Printf("ERROR diagnostic: permission denied. verify read/write permissions for data_dir and shard wal directories.")
	case errors.Is(err, os.ErrNotExist):
		log.Printf("ERROR diagnostic: required path does not exist. verify configured paths and parent directories.")
	case strings.Contains(msg, "invalid meta format") || strings.Contains(msg, "invalid collection meta format") || strings.Contains(msg, "unsupported meta version"):
		log.Printf("ERROR diagnostic: metadata is corrupted or incompatible. restore metadata from backup or remove the corrupted files.")
	case strings.Contains(msg, "wal") && (strings.Contains(msg, "invalid log magic") || strings.Contains(msg, "unsupported log version") || strings.Contains(msg, "read wal header") || strings.Contains(msg, "replay")):
		log.Printf("ERROR diagnostic: wal appears corrupted or incompatible. inspect wal files in shard directories before restarting.")
	case strings.Contains(msg, "create data dir") || strings.Contains(msg, "read data dir") || strings.Contains(msg, "create dir"):
		log.Printf("ERROR diagnostic: invalid or inaccessible storage directory configuration.")
	}

	os.Exit(1)
}

func walModeSummary(writeMode string, walMode storage.WALSyncMode, opts storage.Options) string {
	return fmt.Sprintf("write_mode=%s wal_sync=%s batch_max_ops=%d batch_wait=%s sync_interval=%s",
		writeMode,
		walMode,
		opts.WALBatchMaxOps,
		formatDuration(opts.WALBatchWait),
		formatDuration(opts.WALSyncInterval),
	)
}

func fsyncPolicySummary(walMode storage.WALSyncMode, opts storage.Options) string {
	switch walMode {
	case storage.WALSyncAlways:
		return "fsync on every WAL batch"
	case storage.WALSyncPeriodic:
		return "periodic fsync every " + formatDuration(opts.WALSyncInterval)
	case storage.WALSyncNone:
		return "fsync disabled"
	default:
		return string(walMode)
	}
}

func absoluteOrOriginal(path string) string {
	abs, err := filepath.Abs(path)
	if err != nil {
		return path
	}
	return abs
}

func formatBytes(v uint64) string {
	if v == 0 {
		return "0 B"
	}
	units := []string{"B", "KiB", "MiB", "GiB", "TiB"}
	value := float64(v)
	unit := 0
	for value >= 1024 && unit < len(units)-1 {
		value /= 1024
		unit++
	}
	if unit == 0 {
		return fmt.Sprintf("%d %s", v, units[unit])
	}
	return fmt.Sprintf("%.2f %s", value, units[unit])
}

func formatDuration(d time.Duration) string {
	if d <= 0 {
		return "0s"
	}
	return d.String()
}

func formatCount(n int64) string {
	neg := n < 0
	if neg {
		n = -n
	}
	s := strconv.FormatInt(n, 10)
	if len(s) <= 3 {
		if neg {
			return "-" + s
		}
		return s
	}
	var out []byte
	first := len(s) % 3
	if first == 0 {
		first = 3
	}
	out = append(out, s[:first]...)
	for i := first; i < len(s); i += 3 {
		out = append(out, ',')
		out = append(out, s[i:i+3]...)
	}
	if neg {
		return "-" + string(out)
	}
	return string(out)
}

func formatTime(t time.Time) string {
	if t.IsZero() {
		return "n/a"
	}
	return t.UTC().Format(time.RFC3339)
}

func yesNo(v bool) string {
	if v {
		return "yes"
	}
	return "no"
}
