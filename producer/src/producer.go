package producer

import (
	"context"
	"golang-kafka-lib/constant"
	"golang-kafka-lib/variable"
	"golang-kafka-producer/src/dto"
	"log"
	"strconv"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/protocol"
)

func ProduceMessage(ctx context.Context) (err error) {
	wr := kafka.NewWriter(kafka.WriterConfig{
		Dialer:  kafka.DefaultDialer,
		Brokers: constant.GetBrokers(),
		Topic:   constant.KafkaTopics,
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

func ProduceMessages(ctx context.Context, req dto.ProduceMessagesOpt) (err error) {
	wr := kafka.NewWriter(kafka.WriterConfig{
		Dialer:  kafka.DefaultDialer,
		Brokers: []string{constant.KafkaBrokerSingle},
		Topic:   constant.KafkaTopics,

		Logger:       log.Default(),
		Async:        false,
		RequiredAcks: -1, // to ensure the message is retrieved & cloned (fault-tolerant)
	})
	defer wr.Close()

	log.Println("Start sending message ...")
	messages := make([]kafka.Message, 0, req.TotalMessage)
	for i := 0; i < req.TotalMessage; i++ {
		var val []byte
		if len(req.Messages) == req.TotalMessage {
			val = []byte(req.Messages[i])
		} else {
			val = []byte(req.TemplateMessage + " [" + strconv.Itoa(i) + "]")
		}

		messages = append(messages, kafka.Message{
			Key:   []byte(variable.GetDefault(req.Key, "unassigned")),
			Value: val,
			Headers: []protocol.Header{{
				Key:   "E-TRIP",
				Value: []byte("hihahihi"),
			}},
		})
	}

	err = wr.WriteMessages(ctx, messages...)
	if err != nil {
		log.Fatalf(err.Error())
	}
	log.Println("Done sending message")
	return
}
