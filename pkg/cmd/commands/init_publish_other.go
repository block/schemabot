//go:build !darwin && !linux

package commands

import "fmt"

func renameInitSchema(string, string) error {
	return fmt.Errorf("native initialization currently requires macOS or Linux")
}
