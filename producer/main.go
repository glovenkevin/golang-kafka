package main

import (
	"context"
	producer "golang-kafka-producer/src"
	"golang-kafka-producer/src/dto"
	"os"
	"os/signal"
	"syscall"
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

	// err = producer.ProduceMessage(ctx) // single
	// if err != nil {
	// 	return
	// }

	err = producer.ProduceMessages(ctx, dto.ProduceMessagesOpt{
		TotalMessage:    15,
		Key:             "test",
		TemplateMessage: "Test kirim beda partisi",
	}) // Multiple
	if err != nil {
		return
	}
}
