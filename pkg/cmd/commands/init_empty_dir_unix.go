//go:build darwin || linux

package commands

import "golang.org/x/sys/unix"

func removeEmptyInitDir(path string) error { return unix.Rmdir(path) }
