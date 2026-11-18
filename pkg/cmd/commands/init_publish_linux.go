//go:build linux

package commands

import "golang.org/x/sys/unix"

func renameInitSchema(from, to string) error {
	return unix.Renameat2(unix.AT_FDCWD, from, unix.AT_FDCWD, to, unix.RENAME_NOREPLACE)
}
