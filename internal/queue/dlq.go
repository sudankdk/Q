package queue

// DeadLetterQueue is a special queue that holds messages that have failed to be processed after a certain number of retries.
type DeadLetterQueue struct {
	// underlying queue to hold the dead letter messages
	items []Message
}

func (dlq *DeadLetterQueue) Add(msg Message) {
	dlq.items = append(dlq.items, msg)
}

func (dlq *DeadLetterQueue) GetItems() []Message {
	return dlq.items
}
func (dlq *DeadLetterQueue) Clear() {
	dlq.items = []Message{}
}
