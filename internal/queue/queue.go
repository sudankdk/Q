package queue

import (
	"errors"
	"sync"
	"time"
)

var (
	ErrQueueEmpty       = errors.New("queue is empty")
	ErrDuplicateMessage = errors.New("message already exists")
	ErrMessageNotFound  = errors.New("message not found")
	defaultInFlightTTL  = 30 * time.Second
)

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

func NewQueue() *Queue {
	return &Queue{
		items: make(map[string]*QueueItems),
		order: []string{},
	}
}

func (q *Queue) Enqueue(msg Message) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.ensureInitLocked()
	if _, exists := q.items[msg.Id]; exists {
		return ErrDuplicateMessage
	}
	if msg.CreatedAt == 0 {
		msg.CreatedAt = time.Now().UnixNano()
	}
	q.items[msg.Id] = &QueueItems{
		Message: msg,
		State:   Ready,
	}
	q.order = append(q.order, msg.Id)
	return nil
}

func (q *Queue) Dequeue() (*Message, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.ensureInitLocked()
	for _, msgID := range q.order {
		item, exists := q.items[msgID]
		if !exists || item == nil || item.State != Ready {
			continue
		}
		item.State = InFlight
		item.ExpireAt = time.Now().Add(defaultInFlightTTL).UnixNano()
		msg := item.Message
		return &msg, nil
	}
	return nil, ErrQueueEmpty
}

func (q *Queue) Ack(msgID string) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.ensureInitLocked()
	if _, exists := q.items[msgID]; !exists {
		return ErrMessageNotFound
	}
	delete(q.items, msgID)
	q.removeFromOrderLocked(msgID)
	return nil
}

func (q *Queue) Size() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.items)
}

func (q *Queue) Restore(messages []Message) error {
	for _, msg := range messages {
		if err := q.Enqueue(msg); err != nil {
			return err
		}
	}
	return nil
}

func (q *Queue) ReapExpired(now time.Time, maxRetries int, dlq *DeadLetterQueue) int {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.ensureInitLocked()
	processed := 0
	deadline := now.UnixNano()
	for _, msgID := range append([]string(nil), q.order...) {
		item, exists := q.items[msgID]
		if !exists || item == nil || item.State != InFlight || item.ExpireAt > deadline {
			continue
		}
		processed++
		msg := item.Message
		msg.RetryCount++
		if maxRetries > 0 && msg.RetryCount > maxRetries {
			if dlq != nil {
				dlq.Add(msg)
			}
			delete(q.items, msgID)
			q.removeFromOrderLocked(msgID)
			continue
		}
		item.Message = msg
		item.State = Ready
		item.ExpireAt = 0
	}
	return processed
}

func (q *Queue) ensureInitLocked() {
	if q.items == nil {
		q.items = make(map[string]*QueueItems)
	}
	if q.order == nil {
		q.order = []string{}
	}
}

func (q *Queue) removeFromOrderLocked(msgID string) {
	for index, id := range q.order {
		if id != msgID {
			continue
		}
		q.order = append(q.order[:index], q.order[index+1:]...)
		return
	}
}
