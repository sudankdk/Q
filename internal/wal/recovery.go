package wal

import (
	"bufio"
	"encoding/json"

	"github.com/sudankdk/q/internal/queue"
)

// Recovery is responsible for recovering messages from the WAL log file during startup.
type Recovery struct {
	// path to the wal log file
	Path string
	// wal instance to read the log file
	WAL *WAL
}

func (r *Recovery) Recover() ([]queue.Message, error) {
	messages, err := r.readFromWAL()
	if err != nil {
		return nil, err
	}
	return messages, nil
}

func (r *Recovery) readFromWAL() ([]queue.Message, error) {
	r.WAL.mu.Lock()
	defer r.WAL.mu.Unlock()
	if _, err := r.WAL.file.Seek(0, 0); err != nil {
		return nil, err
	}
	scanner := bufio.NewScanner(r.WAL.file)
	var message []queue.Message
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		var msg queue.Message
		if err := json.Unmarshal(line, &msg); err != nil {
			return nil, err
		}
		message = append(message, msg)

	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return message, nil
}
