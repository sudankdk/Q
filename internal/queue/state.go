package queue


// State represents the state of a message in the queue.
type state int

const (
	Ready state = iota
	InFlight
	Acked
)