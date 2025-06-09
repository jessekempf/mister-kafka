package producer

import (
	"context"
	"math"
	"net"

	"github.com/jessekempf/kafka-go"
	"github.com/jessekempf/mister-kafka/core"
)

type Producer[K any, V any] struct {
	topic  core.ProducerTopic[K, V]
	writer *kafka.Writer
}

func NewProducer[K any, V any](broker net.Addr, topic core.ProducerTopic[K, V]) *Producer[K, V] {
	return &Producer[K, V]{
		topic: topic,
		writer: &kafka.Writer{
			Addr:         broker,
			Balancer:     &kafka.Hash{},
			MaxAttempts:  math.MaxInt,
			RequiredAcks: kafka.RequireAll,
			Async:        false,
			Completion: func(messages []kafka.Message, err error) {
			},
			Compression:            kafka.Gzip, // GZIP gives the best compression supported
			AllowAutoTopicCreation: false,
		},
	}
}

// Produce sends Ts to its Kafka topic.
func (p *Producer[K, V]) Produce(ctx context.Context, payloads ...core.OutboundMessage[K, V]) error {
	messages := make([]kafka.Message, len(payloads))

	for i, payload := range payloads {
		m, err := p.topic.EncodeMessage(payload)

		if err != nil {
			return err
		}

		messages[i] = *m
	}

	return p.writer.WriteMessages(ctx, messages...)
}
