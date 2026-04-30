//go:build !windows

package localscale

import (
	"syscall"

	"golang.org/x/sys/unix"
)

// setReuseAddr sets SO_REUSEADDR on the socket so that proxy ports can be
// reused immediately after close, avoiding TIME_WAIT exhaustion when tests
// rapidly create and tear down branch proxies on a fixed port range.
func setReuseAddr(network, address string, c syscall.RawConn) error {
	var setErr error
	err := c.Control(func(fd uintptr) {
		setErr = unix.SetsockoptInt(int(fd), unix.SOL_SOCKET, unix.SO_REUSEADDR, 1)
	})
	if err != nil {
		return err
	}
	return setErr
}
