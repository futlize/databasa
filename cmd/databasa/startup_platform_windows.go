//go:build windows

package main

import (
	"fmt"

	"golang.org/x/sys/windows"
)

func diskFreeBytes(path string) (uint64, error) {
	ptr, err := windows.UTF16PtrFromString(path)
	if err != nil {
		return 0, err
	}
	var freeForCaller uint64
	var totalBytes uint64
	var totalFree uint64
	if err := windows.GetDiskFreeSpaceEx(ptr, &freeForCaller, &totalBytes, &totalFree); err != nil {
		return 0, err
	}
	return totalFree, nil
}

func kernelVersion() string {
	ver, err := windows.GetVersion()
	if err != nil {
		return "unknown"
	}
	major := ver & 0xff
	minor := (ver >> 8) & 0xff
	build := (ver >> 16) & 0xffff
	return fmt.Sprintf("%d.%d.%d", major, minor, build)
}
