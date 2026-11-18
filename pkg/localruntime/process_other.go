//go:build !darwin && !linux

package localruntime

import (
	"fmt"
	"os"
	"os/exec"
)

func lock(string) (*os.File, bool, error) {
	return nil, false, fmt.Errorf("native local runtimes currently require macOS or Linux")
}
func inheritedLock(string) (*os.File, error) {
	return nil, fmt.Errorf("native local runtimes currently require macOS or Linux")
}
func detach(*exec.Cmd, *os.File) {}
