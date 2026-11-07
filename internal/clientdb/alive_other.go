//go:build !windows

package clientdb

import (
	"errors"
	"os"
	"syscall"
)

// ProcessAlive reports whether a pid is running.
func ProcessAlive(pid int) bool {
	if pid <= 0 {
		return false
	}
	p, err := os.FindProcess(pid)
	if err != nil {
		return false
	}
	err = p.Signal(syscall.Signal(0))
	return err == nil || errors.Is(err, os.ErrPermission)
}
