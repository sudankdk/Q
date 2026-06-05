package worker

import (
	"time"

	"github.com/sudankdk/q/internal/queue"
)

// reaper is responsible for reaping messages that have been in-flight for too long and have not been acknowledged.
// It runs periodically and checks for messages that have been in-flight for longer than a specified timeout.
// If it finds such messages, it re-enqueues them back to the queue for processing.
type reaper struct {
	// 
	q queue.Queue
	//
	interval time.Duration
}


// start starts the reaper to run periodically and check for messages that have been in-flight for too long.
func (r *reaper) start(){
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			r.reap()
		}
	}
}


// reap checks for messages that have been in-flight for longer than the specified timeout and re-enqueues them back to the queue.
func (r *reaper) reap(){
	
}