// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package localfileio

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/larksuite/cli/extension/fileio"
	"github.com/larksuite/cli/internal/charcheck"
	"github.com/larksuite/cli/internal/vfs"
)

// Provider is the default fileio.Provider backed by the local filesystem.
type Provider struct{}

func (p *Provider) Name() string { return "local" }

func (p *Provider) ResolveFileIO(_ context.Context) fileio.FileIO {
	return &LocalFileIO{}
}

func init() {
	fileio.Register(&Provider{})
}

// LocalFileIO implements fileio.FileIO using the local filesystem.
// Path validation (SafeInputPath/SafeOutputPath), directory creation,
// and atomic writes are handled internally.
type LocalFileIO struct{}

var _ fileio.TempDirFileCreator = (*LocalFileIO)(nil)

// Open opens a local file for reading after validating the path.
func (l *LocalFileIO) Open(name string) (fileio.File, error) {
	safePath, err := SafeInputPath(name)
	if err != nil {
		return nil, &fileio.PathValidationError{Err: err}
	}
	return vfs.Open(safePath)
}

// Stat returns file metadata after validating the path.
func (l *LocalFileIO) Stat(name string) (fileio.FileInfo, error) {
	safePath, err := SafeInputPath(name)
	if err != nil {
		return nil, &fileio.PathValidationError{Err: err}
	}
	return vfs.Stat(safePath)
}

// saveResult implements fileio.SaveResult.
type saveResult struct{ size int64 }

func (r *saveResult) Size() int64 { return r.size }

// ResolvePath returns the validated absolute path for the given output path.
func (l *LocalFileIO) ResolvePath(path string) (string, error) {
	resolved, err := SafeOutputPath(path)
	if err != nil {
		return "", &fileio.PathValidationError{Err: err}
	}
	return resolved, nil
}

// CreateTempDirFile atomically creates a unique directory in the current
// working directory, then creates the requested empty file inside it.
func (l *LocalFileIO) CreateTempDirFile(directoryPattern, fileName string) (string, error) {
	if err := validateTempDirectoryPattern(directoryPattern); err != nil {
		return "", &fileio.PathValidationError{Err: err}
	}
	if err := validateTempFileName(fileName); err != nil {
		return "", &fileio.PathValidationError{Err: err}
	}
	tempDir, err := vfs.MkdirTemp(".", directoryPattern)
	if err != nil {
		return "", &fileio.MkdirError{Err: err}
	}
	path := filepath.Join(tempDir, fileName)
	tempFile, err := vfs.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		_ = vfs.RemoveAll(tempDir)
		return "", &fileio.WriteError{Err: err}
	}
	if err := tempFile.Close(); err != nil {
		_ = vfs.RemoveAll(tempDir)
		return "", &fileio.WriteError{Err: fmt.Errorf("close temporary file: %w", err)}
	}
	return filepath.Join(filepath.Base(tempDir), fileName), nil
}

func validateTempDirectoryPattern(pattern string) error {
	if strings.TrimSpace(pattern) == "" || strings.ContainsAny(pattern, `/\\`) || strings.Count(pattern, "*") != 1 {
		return fmt.Errorf("temporary directory pattern must be one non-empty path component containing exactly one '*'")
	}
	return charcheck.RejectControlChars(pattern, "temporary directory pattern")
}

func validateTempFileName(fileName string) error {
	if strings.TrimSpace(fileName) == "" || fileName != filepath.Base(fileName) || strings.ContainsAny(fileName, "/\\\t\r\n") {
		return fmt.Errorf("temporary file name must be one non-empty path component")
	}
	return charcheck.RejectControlChars(fileName, "temporary file name")
}

// Save writes body to path atomically after validating the output path.
// Parent directories are created as needed. The body is streamed directly
// to a temp file and renamed, avoiding full in-memory buffering.
func (l *LocalFileIO) Save(path string, _ fileio.SaveOptions, body io.Reader) (fileio.SaveResult, error) {
	safePath, err := SafeOutputPath(path)
	if err != nil {
		return nil, &fileio.PathValidationError{Err: err}
	}
	if err := vfs.MkdirAll(filepath.Dir(safePath), 0700); err != nil {
		return nil, &fileio.MkdirError{Err: err}
	}
	n, err := AtomicWriteFromReader(safePath, body, 0600)
	if err != nil {
		return nil, &fileio.WriteError{Err: err}
	}
	return &saveResult{size: n}, nil
}
