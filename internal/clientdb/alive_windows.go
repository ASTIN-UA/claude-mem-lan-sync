//go:build windows

package clientdb

import (
	"errors"
	"syscall"
)

// processQueryLimitedInformation is PROCESS_QUERY_LIMITED_INFORMATION, the
// least access that still allows GetExitCodeProcess.
const processQueryLimitedInformation = 0x1000

// stillActive is STILL_ACTIVE, the exit code of a process that has not exited.
const stillActive = 259

// ProcessAlive reports whether a pid is running.
//
// os.Process.Signal supports only Kill on Windows, so the signal-0 probe used
// elsewhere always failed here and doctor reported a running worker as stopped.
func ProcessAlive(pid int) bool {
	if pid <= 0 {
		return false
	}
	h, err := syscall.OpenProcess(processQueryLimitedInformation, false, uint32(pid))
	if err != nil {
		// Access denied means the process exists but belongs to someone else.
		return errors.Is(err, syscall.ERROR_ACCESS_DENIED)
	}
	defer func() { _ = syscall.CloseHandle(h) }()
	var code uint32
	if err := syscall.GetExitCodeProcess(h, &code); err != nil {
		return false
	}
	return code == stillActive
}
