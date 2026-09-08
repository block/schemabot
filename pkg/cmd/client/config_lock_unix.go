//go:build darwin || linux || freebsd || openbsd || netbsd || dragonfly || solaris

package client

import (
	"fmt"
	"os"
	"syscall"

	"github.com/block/spirit/pkg/utils"
	"golang.org/x/sys/unix"
)

// Never unlink the lock file: all writers must agree on the same inode.
func lockConfig(path string) (*os.File, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|syscall.O_NOFOLLOW, 0600)
	if err != nil {
		return nil, fmt.Errorf("open config lock: %w", err)
	}
	info, err := f.Stat()
	if err != nil {
		utils.CloseAndLog(f)
		return nil, fmt.Errorf("inspect config lock %s: %w", path, err)
	}
	if !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 {
		utils.CloseAndLog(f)
		return nil, fmt.Errorf("config lock %s must be a private regular file", path)
	}
	if err := unix.Flock(int(f.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		utils.CloseAndLog(f)
		return nil, fmt.Errorf("CLI configuration is busy; retry the command: %w", err)
	}
	return f, nil
}
