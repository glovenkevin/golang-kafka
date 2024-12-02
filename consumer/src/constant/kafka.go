package constant

const (
	KafkaBroker = "localhost:29092"
	KafkaTopics = "tr_trip"
	// GroupID is mandatory if you try to read message and manually commit it.
	// Otherwise you can use direct commit / interval commit set in kafka readers
	KafkaGroupId = "trip_test"
)
