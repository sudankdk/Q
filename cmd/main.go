package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/sudankdk/q/internal/queue"
	"github.com/sudankdk/q/internal/wal"
	"github.com/sudankdk/q/internal/worker"
)

func main() {
	fmt.Println("Q is a queue system written in Go, designed to be simple and efficient.")

	walPath := filepath.Join("data", "queue.wal")
	logStore, err := wal.NewWAL(walPath)
	if err != nil {
		log.Fatal(err)
	}
	defer logStore.Close()

	recovery := wal.Recovery{WAL: logStore}
	recovered, err := recovery.Recover()
	if err != nil {
		log.Fatal(err)
	}

	q := queue.NewQueue()
	if err := q.Restore(recovered); err != nil {
		log.Fatal(err)
	}

	dlq := &queue.DeadLetterQueue{}
	reaper := worker.NewReaper(q, dlq, time.Second, 3)
	reaper.Reap()

	sample := queue.Message{Id: "demo-1", Payload: []byte("hello from q")}
	if err := q.Enqueue(sample); err != nil {
		log.Fatal(err)
	}
	if err := logStore.Append(sample); err != nil {
		log.Fatal(err)
	}

	msg, err := q.Dequeue()
	if err != nil {
		log.Fatal(err)
	}
	if err := q.Ack(msg.Id); err != nil {
		log.Fatal(err)
	}

	fmt.Fprintf(os.Stdout, "processed message %s, queue size=%d, dlq size=%d\n", msg.Id, q.Size(), len(dlq.GetItems()))
}
