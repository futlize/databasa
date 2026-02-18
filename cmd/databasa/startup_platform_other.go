//go:build !unix && !windows

package main

import "fmt"

func diskFreeBytes(path string) (uint64, error) {
	return 0, fmt.Errorf("disk free bytes not supported on this platform")
}

func kernelVersion() string {
	return "unknown"
}
