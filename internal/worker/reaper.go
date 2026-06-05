package worker

import (
	"time"

	"github.com/sudankdk/q/internal/queue"
)

// Reaper is responsible for reaping messages that have been in-flight for too long and have not been acknowledged.
// It runs periodically and checks for messages that have been in-flight for longer than a specified timeout.
// If it finds such messages, it re-enqueues them back to the queue for processing.
type Reaper struct {
	q          *queue.Queue
	dlq        *queue.DeadLetterQueue
	interval   time.Duration
	maxRetries int
}

func NewReaper(q *queue.Queue, dlq *queue.DeadLetterQueue, interval time.Duration, maxRetries int) *Reaper {
	return &Reaper{
		q:          q,
		dlq:        dlq,
		interval:   interval,
		maxRetries: maxRetries,
	}
}

// start starts the reaper to run periodically and check for messages that have been in-flight for too long.
func (r *Reaper) Start() {
	if r == nil {
		return
	}
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			r.Reap()
		}
	}
}

// reap checks for messages that have been in-flight for longer than the specified timeout and re-enqueues them back to the queue.
func (r *Reaper) Reap() {
	if r == nil || r.q == nil {
		return
	}
	r.q.ReapExpired(time.Now(), r.maxRetries, r.dlq)
}
