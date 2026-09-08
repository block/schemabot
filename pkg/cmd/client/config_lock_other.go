//go:build !darwin && !linux && !windows && !freebsd && !openbsd && !netbsd && !dragonfly

package client

import (
	"fmt"
	"os"
)

func lockConfig(string) (*os.File, error) {
	return nil, fmt.Errorf("safe CLI configuration updates currently are unsupported on this platform")
}
