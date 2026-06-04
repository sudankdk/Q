package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFIFO(t *testing.T) {
	q := &InMemoryQueue{}
	msg1 := Message{Id: "1", Payload: []byte("Hello")}
	msg2 := Message{Id: "2", Payload: []byte("World")}
	q.Enqueue(msg1)
	q.Enqueue(msg2)
	dequeuedMsg1, _ := q.Dequeue()
	dequeuedMsg2, _ := q.Dequeue()
	if dequeuedMsg1.Id != "1" || dequeuedMsg2.Id != "2" {
		t.Fatal("FIFO test failed")
	}
	assert.Equal(t, "1", dequeuedMsg1.Id)
	assert.Equal(t, "2", dequeuedMsg2.Id)
}
