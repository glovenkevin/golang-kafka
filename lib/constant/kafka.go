package constant

const (
	KafkaBroker01 = "localhost:19092"
	KafkaBroker02 = "localhost:29092"

	KafkaTopics = "tr_trip"
	// GroupID is mandatory if you try to read message and manually commit it.
	// Otherwise you can use direct commit / interval commit set in kafka readers
	KafkaGroupId = "trip_test"
)

func GetBrokers() []string {
	return []string{
		KafkaBroker01, KafkaBroker02,
	}
}
