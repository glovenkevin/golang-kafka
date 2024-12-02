package consumer

import (
	"context"
	"golang-kafka-consumer/src/constant"
	"golang-kafka-consumer/src/dto"
	"log"

	"github.com/segmentio/kafka-go"
)

func FetchMessage(ctx context.Context, opt dto.ConsumerOpt) (err error) {
	retryCount := 1
	if opt.RetryTime > 1 {
		retryCount = opt.RetryTime
	}

	for i := 0; i < retryCount; i++ {
		r := kafka.NewReader(kafka.ReaderConfig{
			Brokers:               []string{constant.KafkaBroker},
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
