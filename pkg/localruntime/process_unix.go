//go:build darwin || linux

package localruntime

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"

	"github.com/block/spirit/pkg/utils"
	"golang.org/x/sys/unix"
)

// The inherited open file description owns flock across exec and parent exit.
// Never unlink this file: changing its inode would create two lock authorities.
func lock(dir string) (*os.File, bool, error) {
	f, err := os.OpenFile(filepath.Join(dir, "process.lock"), os.O_CREATE|os.O_RDWR|syscall.O_NOFOLLOW, 0600)
	if err != nil {
		return nil, false, err
	}
	info, err := f.Stat()
	if err != nil {
		utils.CloseAndLog(f)
		return nil, false, err
	}
	if !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 {
		utils.CloseAndLog(f)
		return nil, false, fmt.Errorf("runtime lock must be a private regular file")
	}
	err = unix.Flock(int(f.Fd()), unix.LOCK_EX|unix.LOCK_NB)
	if errors.Is(err, unix.EWOULDBLOCK) {
		utils.CloseAndLog(f)
		return nil, false, nil
	}
	if err != nil {
		utils.CloseAndLog(f)
		return nil, false, err
	}
	return f, true, nil
}

func detach(cmd *exec.Cmd, lease *os.File) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	cmd.ExtraFiles = []*os.File{lease}
}

func inheritedLock(dir string) (*os.File, error) {
	f := os.NewFile(3, "runtime lease")
	if f == nil {
		return nil, fmt.Errorf("runtime lease was not inherited")
	}
	info, err := f.Stat()
	if err != nil {
		utils.CloseAndLog(f)
		return nil, err
	}
	expected, err := os.Stat(filepath.Join(dir, "process.lock"))
	if err != nil {
		utils.CloseAndLog(f)
		return nil, err
	}
	if !os.SameFile(info, expected) {
		utils.CloseAndLog(f)
		return nil, fmt.Errorf("runtime lease does not match its directory")
	}
	if err := unix.Flock(int(f.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		utils.CloseAndLog(f)
		return nil, fmt.Errorf("runtime lease unavailable: %w", err)
	}
	return f, nil
}
