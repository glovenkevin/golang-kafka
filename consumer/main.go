package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/segmentio/kafka-go"
)

const (
	KafkaBroker = "localhost:29092"
	KafkaTopics = "tr_trip"
	// GroupID is mandatory if you try to read message and manually commit it.
	// Otherwise you can use direct commit / interval commit set in kafka readers
	KafkaGroupId = "trip_test"
)

func main() {
	ctx := context.Background()
	var err error

	ctx, cancel := signal.NotifyContext(ctx, os.Interrupt, os.Kill, syscall.SIGTERM)
	defer cancel()

	err = FetchMessage(ctx)
	if err != nil {
		return
	}
}

func FetchMessage(ctx context.Context) (err error) {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:               []string{KafkaBroker},
		Topic:                 KafkaTopics,
		MaxBytes:              10e6, // 10MB
		StartOffset:           kafka.LastOffset,
		GroupID:               KafkaTopics,
		WatchPartitionChanges: true,
	})

	messageChan := make(chan *kafka.Message)
	log.Println("start listening ...")
	go func() {
		defer close(messageChan)
		for {
			message, err := r.FetchMessage(ctx)
			if err != nil {
				log.Fatal(err)
				break
			}

			select {
			case <-ctx.Done():
				break
			case messageChan <- &message:
			}
		}
	}()

	go func() {
		for msg := range messageChan {
			select {
			case <-ctx.Done():
				break
			default:
			}

			log.Println("key:", string(msg.Key))
			log.Println("value:", string(msg.Value))
			log.Println("header:", msg.Headers)

			// if err = r.CommitMessages(ctx, *msg); err != nil {
			// 	log.Println("fail commit:", err)
			// }
		}
	}()

	select {
	case <-ctx.Done():
		log.Println("Gracefull Shutdown")
	}

	if err = r.Close(); err != nil {
		log.Fatal(err)
	}
	return
}
