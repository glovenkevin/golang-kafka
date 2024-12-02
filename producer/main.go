package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/protocol"
)

const (
	KafkaBroker = "localhost:29092"
	KafkaTopics = "tr_trip"
)

func main() {
	ctx := context.Background()
	var err error

	ctx, cancel := signal.NotifyContext(ctx, os.Interrupt, os.Kill, syscall.SIGTERM)
	defer cancel()

	err = ProduceMessage(ctx)
	if err != nil {
		return
	}
}

func ProduceMessage(ctx context.Context) (err error) {
	wr := kafka.NewWriter(kafka.WriterConfig{
		Dialer:  kafka.DefaultDialer,
		Brokers: []string{KafkaBroker},
		Topic:   KafkaTopics,
	})
	defer wr.Close()

	log.Println("Start sending message ...")
	err = wr.WriteMessages(ctx, kafka.Message{
		Key:   []byte("unassigned"),
		Value: []byte("Masok pak Eko"),
		Headers: []protocol.Header{{
			Key:   "E-TRIP",
			Value: []byte("hihahihi"),
		}},
	})
	if err != nil {
		log.Fatalf(err.Error())
	}
	log.Println("Done sending message")
	return
}
