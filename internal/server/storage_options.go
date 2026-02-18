package server

import (
	"sync"

	"github.com/futlize/databasa/internal/storage"
)

var (
	storageOptionsMu sync.RWMutex
	storageOptions   = storage.DefaultOptions()
)

func DefaultStorageOptions() storage.Options {
	storageOptionsMu.RLock()
	opts := storageOptions
	storageOptionsMu.RUnlock()
	return opts
}

func SetDefaultStorageOptions(opts storage.Options) storage.Options {
	storageOptionsMu.Lock()
	storageOptions = opts
	storageOptionsMu.Unlock()
	return opts
}

func currentStorageOptions() storage.Options {
	storageOptionsMu.RLock()
	opts := storageOptions
	storageOptionsMu.RUnlock()
	return opts
}

