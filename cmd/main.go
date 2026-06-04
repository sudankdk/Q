package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

func main() {
	fmt.Println("Q is a queue system written in Go, designed to be simple and efficient.")

	message1 := Message{Id: "1", Payload: []byte("Hello")}
	message2 := Message{Id: "2", Payload: []byte("World")}
	// Determine data file path relative to the executable so the program
	// can be run from any working directory.
	exe, err := os.Executable()
	if err != nil {
		fmt.Printf("failed to determine executable path: %v\n", err)
		return
	}
	exeDir := filepath.Dir(exe)
	dataPath := filepath.Join(exeDir, "data", "queue.log")

	f, err := os.OpenFile(dataPath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		fmt.Printf("failed to open WAL file %s: %v\n", dataPath, err)
		return
	}
	defer f.Close()

	wal := &WAL{file: f}
	msg, err := wal.ReadFromWAL()
	if err != nil {
		fmt.Printf("Error reading from WAL: %v\n", err)
		return
	}

	for _, m := range msg {
		fmt.Printf("Recovered message from WAL: %s\n", m.Id)
		fmt.Printf("message from WAL: %s\n", string(m.Payload))
	}

	pq := &PersistentQueue{wal: wal}
	err = pq.Enqueue(message1)
	if err != nil {
		fmt.Printf("Error enqueuing message: %v\n", err)
		return
	}
	err = pq.Enqueue(message2)
	if err != nil {
		fmt.Printf("Error enqueuing message: %v\n", err)
		return
	}
	dequeuedMsg1, err := pq.Dequeue()
	if err != nil {
		fmt.Printf("Error dequeuing message: %v\n", err)
		return
	}
	dequeuedMsg2, err := pq.Dequeue()
	if err != nil {
		fmt.Printf("Error dequeuing message: %v\n", err)
		return
	}
	fmt.Printf("Dequeued message: %s\n", dequeuedMsg1.Id)
	fmt.Printf("Dequeued message: %s\n", dequeuedMsg2.Id)
}

// messages
type Message struct {
	Id      string
	Payload []byte
}

// Queue interface
type Queue interface {
	Enqueue(msg Message) error
	Dequeue() (*Message, error)
	Size() int
}

// // In-memory queue implementation
// type InMemoryQueue struct {
// 	mu       sync.Mutex
// 	messages []Message
// }

// // Enqueue adds a message to the queue
// func (q *InMemoryQueue) Enqueue(msg Message) error {
// 	q.mu.Lock()
// 	defer q.mu.Unlock()
// 	if len(q.messages) >= 1000 {
// 		return fmt.Errorf("queue is full")
// 	}

// 	if msg.Id == "" {
// 		return fmt.Errorf("message ID cannot be empty")
// 	}

// 	if len(msg.Payload) == 0 {
// 		return fmt.Errorf("message payload cannot be empty")
// 	}
// 	q.messages = append(q.messages, msg)
// 	fmt.Printf("Enqueued message: %s\n", msg.Id)
// 	return nil

// }

// // Dequeue removes and returns the first message from the queue
// func (q *InMemoryQueue) Dequeue() (*Message, error) {
// 	q.mu.Lock()
// 	defer q.mu.Unlock()
// 	if len(q.messages) == 0 {
// 		return nil, fmt.Errorf("queue is empty")
// 	}
// 	msg := q.messages[0]
// 	if len(msg.Payload) == 0 {
// 		return &Message{}, fmt.Errorf("message payload cannot be empty")
// 	}
// 	q.messages = q.messages[1:]
// 	fmt.Printf("Dequeued message: %s\n", msg.Id)
// 	return &msg, nil
// }

// WAL simple
type WAL struct {
	mu   sync.Mutex
	file *os.File
}

// appender
func (w *WAL) Append(msg Message) error {
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

// reader
func (w *WAL) ReadFromWAL() ([]Message, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	var messages []Message
	if _, err := w.file.Seek(0, 0); err != nil { // Move to the beginning of the file
		return nil, err
	}

	scanner := bufio.NewScanner(w.file)
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		var message Message
		if err := json.Unmarshal(line, &message); err != nil {
			return nil, err
		}
		messages = append(messages, message)
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return messages, nil
}

// persistance memeory
type PersistentQueue struct {
	mu       sync.Mutex
	wal      *WAL
	Messages []Message
}

func (pq *PersistentQueue) Enqueue(msg Message) error {
	pq.mu.Lock()
	defer pq.mu.Unlock()
	err := pq.wal.Append(msg)
	if err != nil {
		return err
	}
	pq.Messages = append(pq.Messages, msg)
	return nil
}

func (pq *PersistentQueue) Dequeue() (*Message, error) {
	pq.mu.Lock()
	defer pq.mu.Unlock()
	if len(pq.Messages) == 0 {
		return nil, fmt.Errorf("queue is empty")
	}

	msg := pq.Messages[0]
	pq.Messages = pq.Messages[1:]
	return &msg, nil
}
