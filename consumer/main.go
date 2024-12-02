package main

import (
	"context"
	consumer "golang-kafka-consumer/src"
	"golang-kafka-consumer/src/dto"
	"log"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	ctx := context.Background()
	var err error

	ctx, cancel := signal.NotifyContext(ctx, os.Interrupt, os.Kill, syscall.SIGTERM)
	defer cancel()

	err = consumer.FetchMessage(ctx, dto.ConsumerOpt{
		RetryTime: 3,
	})
	if err != nil {
		return
	}
	log.Println("Gracefull Shutdown")
}
