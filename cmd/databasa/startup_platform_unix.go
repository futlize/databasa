//go:build unix

package main

import (
	"bytes"
	"strings"

	"golang.org/x/sys/unix"
)

func diskFreeBytes(path string) (uint64, error) {
	var stat unix.Statfs_t
	if err := unix.Statfs(path, &stat); err != nil {
		return 0, err
	}
	return stat.Bavail * uint64(stat.Bsize), nil
}

func kernelVersion() string {
	var uname unix.Utsname
	if err := unix.Uname(&uname); err != nil {
		return "unknown"
	}
	return cString(uname.Release[:])
}

func cString(b []byte) string {
	if i := bytes.IndexByte(b, 0); i >= 0 {
		b = b[:i]
	}
	return strings.TrimSpace(string(b))
}
