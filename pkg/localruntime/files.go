// Package localruntime manages the identity and lifetime of a native runtime.
// Planning, execution, and recovery remain in the shared server and engines.
package localruntime

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"regexp"

	"github.com/block/spirit/pkg/utils"
)

var validID = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_-]{0,63}$`)

// Directory resolves a stable runtime identity within the user's installation.
func Directory(id string) (string, error) {
	if !validID.MatchString(id) {
		return "", fmt.Errorf("invalid local runtime ID: use 1–64 letters, digits, underscores, or dashes")
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(home, ".schemabot", "runtimes", id), nil
}

func privateDirectory(dir string) error {
	if err := os.MkdirAll(dir, 0700); err != nil {
		return err
	}
	info, err := os.Lstat(dir)
	if err != nil {
		return err
	}
	if !info.IsDir() || info.Mode().Perm()&0077 != 0 {
		return fmt.Errorf("runtime directory must be a private directory: %s", dir)
	}
	return nil
}

// ReadPrivate reads a bounded regular file from the same descriptor whose
// permissions are checked. Neither its contents nor parser errors are logged.
func ReadPrivate(path string) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer utils.CloseAndLog(f)
	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 {
		return nil, fmt.Errorf("%s must be a private regular file", path)
	}
	data, err := io.ReadAll(io.LimitReader(f, (1<<20)+1))
	if err != nil {
		return nil, err
	}
	if len(data) > 1<<20 {
		return nil, fmt.Errorf("%s exceeds the 1 MiB file size limit", path)
	}
	return data, nil
}

func writeJSON(path string, value any) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return writeAtomic(path, data)
}

func writeAtomic(path string, data []byte) error {
	f, err := os.CreateTemp(filepath.Dir(path), ".runtime-*")
	if err != nil {
		return err
	}
	defer func() {
		if err := os.Remove(f.Name()); err != nil && !os.IsNotExist(err) {
			slog.Error("remove temporary runtime file", "error", err)
		}
	}()
	if _, err := f.Write(data); err != nil {
		utils.CloseAndLog(f)
		return err
	}
	if err := f.Sync(); err != nil {
		utils.CloseAndLog(f)
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(f.Name(), path)
}

func randomID() (string, error) {
	var b [32]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}
func digest(data []byte) string { sum := sha256.Sum256(data); return hex.EncodeToString(sum[:]) }
func binaryDigest(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer utils.CloseAndLog(f)
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}
