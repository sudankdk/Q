package queue

// Message represents a message in the queue.
type Message struct {
	// Id is a unique identifier for the message.
	Id 	string
	// Payload is the actual content of the message.
	Payload []byte
	// number of retries for the message
	RetryCount int
	// time of the message when it was created or enqueued basically
	CreatedAt int64 
}