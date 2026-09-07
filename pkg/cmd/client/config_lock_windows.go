//go:build windows

package client

import (
	"fmt"
	"os"

	"github.com/block/spirit/pkg/utils"
	"golang.org/x/sys/windows"
)

func lockConfig(path string) (*os.File, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, fmt.Errorf("open config lock: %w", err)
	}
	var overlapped windows.Overlapped
	if err := windows.LockFileEx(windows.Handle(f.Fd()), windows.LOCKFILE_EXCLUSIVE_LOCK|windows.LOCKFILE_FAIL_IMMEDIATELY, 0, 1, 0, &overlapped); err != nil {
		utils.CloseAndLog(f)
		return nil, fmt.Errorf("CLI configuration is busy; retry the command: %w", err)
	}
	return f, nil
}
