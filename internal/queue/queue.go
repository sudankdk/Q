package queue

import "sync"

// metadata for the message in the queue
type QueueItems struct {
	// message in the queue
	Message Message
	// state of the message in the queue
	State state
	// time of the message it expires
	ExpireAt int64
}

// Actual Queue struct
type Queue struct {
	// mutex to protect the queue data structure
	mu sync.Mutex
	// items in the queue, key is the message ID
	items map[string]*QueueItems
	// order of the messages in the queue, used for maintaining the order of messages
	order []string
}

// Queue Interface
type QueueInterface interface {
	Enqueue(msg Message) error
	Dequeue() (*Message, error)
	Ack(msgId string) error
	Size() int
}
