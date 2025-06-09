package main

import (
	"context"
	"log"
	"net"

	"github.com/jessekempf/kafka-go"
	"github.com/jessekempf/mister-kafka/consumer"
	"github.com/jessekempf/mister-kafka/core"
	"github.com/jessekempf/mister-kafka/utils/cli"
)

// GenericKafkaMessage preserves the basic structure of a Kafka message
type GenericKafkaMessage struct {
	key     []byte
	value   []byte
	headers []kafka.Header
}

func main() {
	// Pull configuration from the shell environment
	kafkaAddrStr := cli.GetEnvStr("KAFKA_ADDR")
	kafkaGroup := cli.GetEnvStr("KAFKA_GROUP")
	kafkaTopicName := cli.GetEnvStr("KAFKA_TOPIC")

	// A ConsumerTopic[T] consists of both a topic name and a decoder that has access to the
	// three fields of a Kafka record payload (key, value, and headers)
	kafkaTopic := core.DeclareConsumerTopic(
		kafkaTopicName,
		func(key, value []byte, headers []kafka.Header) (*GenericKafkaMessage, error) {
			return &GenericKafkaMessage{
				key:     key,
				value:   value,
				headers: headers,
			}, nil
		},
	)

	// Resolve the Kafka broker address to a net.Addr.
	kafkaAddr, err := net.ResolveTCPAddr("tcp", kafkaAddrStr)

	if err != nil {
		log.Fatal(err)
	}

	// Create a new consumer and set it infinitely (or at least until ^C, which Consume will trap) consuming messages
	// from the topic.
	err = consumer.
		NewKafkaConsumer(kafkaAddr, kafkaGroup, kafkaTopic).
		Consume(context.Background(), func(ctx context.Context, message *core.InboundMessage[GenericKafkaMessage]) error {
			log.Printf(
				"%s.%d: offset %d, timestamp %s: %s -> %s %s\n",
				message.Topic,
				message.Partition,
				message.Offset,
				message.Time,
				message.Body.key,
				message.Body.headers,
				message.Body.value,
			)
			return nil
		})

	if err != nil {
		log.Printf("error: %s", err)
	}
}
