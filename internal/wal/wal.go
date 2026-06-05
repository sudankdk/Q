package wal

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync"

	"github.com/sudankdk/q/internal/queue"
)

// WAL (Write-Ahead Log) is a simple implementation of a write-ahead log for a queue system.
// inspired from postgres WAL
type WAL struct {
	mu   sync.Mutex
	file *os.File
}

// NewWAL creates a new WAL instance with the given  path.
func NewWAL(path string) (*WAL, error) {
	dir := filepath.Dir(path)

	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}

	file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	return &WAL{file: file}, nil
}

// closes the wal log file
func (w *WAL) Close() error {
	return w.file.Close()
}

// appender
func (w *WAL) Append(msg queue.Message) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	data, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	_, err = w.file.Write(append(data, '\n'))
	if err != nil {
		return err
	}
	return nil
}
