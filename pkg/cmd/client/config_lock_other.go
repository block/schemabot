//go:build !darwin && !linux && !windows

package client

import (
	"fmt"
	"os"
)

func lockConfig(string) (*os.File, error) {
	return nil, fmt.Errorf("safe CLI configuration updates currently are unsupported on this platform")
}
