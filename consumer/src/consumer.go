package consumer

import (
	"context"
	"errors"
	"golang-kafka-consumer/src/dto"
	"golang-kafka-lib/constant"
	"log"

	"github.com/segmentio/kafka-go"
)

// support read commited - which is for failure purpose
// required data is the group_id should be set (it is being assumed that it was a group of consumer)
// source: https://github.com/segmentio/kafka-go?tab=readme-ov-file#explicit-commits
func FetchMessage(ctx context.Context, opt dto.ConsumerOpt) (err error) {
	retryCount := 1
	if opt.RetryTime > 1 {
		retryCount = opt.RetryTime
	}

	for i := 0; i < retryCount; i++ {
		r := kafka.NewReader(kafka.ReaderConfig{
			Brokers:               constant.GetBrokers(),
			Topic:                 constant.KafkaTopics,
			MaxBytes:              10e6, // 10MB
			StartOffset:           kafka.LastOffset,
			GroupID:               constant.KafkaTopics,
			WatchPartitionChanges: true,
			IsolationLevel:        kafka.ReadCommitted,

			// Logger:      log.Default(),
			ErrorLogger: log.Default(),
		})

		log.Println("start listening ...")
		for {
			var message kafka.Message
			message, err = r.FetchMessage(ctx)
			if err != nil {
				log.Fatal(err)
				break
			}

			log.Println("key:", string(message.Key))
			log.Println("value:", string(message.Value))
			log.Println("header:", message.Headers)

			if err = r.CommitMessages(ctx, message); err != nil {
				break
			}

			// err = errors.New("Fail internal")
			// break // testing failed
		}

		if err != nil {
			log.Println("retrying...", i+1)
		}

		if errClose := r.Close(); errClose != nil {
			log.Fatal(errClose)
			break
		}
	}

	if retryCount == 0 {
		log.Println("retry count depleted, stopping programs...")
	}
	return
}

// this will always commit the message to the kafka, the offest will always being updated
func ReadMessage(ctx context.Context, opt dto.ConsumerOpt) (err error) {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:               constant.GetBrokers(),
		Topic:                 constant.KafkaTopics,
		MaxBytes:              10e6, // 10MB
		StartOffset:           kafka.LastOffset,
		GroupID:               constant.KafkaTopics,
		WatchPartitionChanges: true,
		IsolationLevel:        kafka.ReadCommitted,

		// Logger:      log.Default(),
		ErrorLogger: log.Default(),
	})

	log.Println("start listening ...")
	for {
		var message kafka.Message
		message, err = r.ReadMessage(ctx)
		if err != nil && !errors.Is(err, context.Canceled) {
			log.Fatal(err)
			break
		}
		if errors.Is(err, context.Canceled) {
			break
		}

		log.Println("key:", string(message.Key))
		log.Println("value:", string(message.Value))
		log.Println("header:", message.Headers)
	}

	if errClose := r.Close(); errClose != nil {
		log.Fatal(errClose)
	}
	return
}
