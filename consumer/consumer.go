package consumer

import (
	"context"
	"net"

	"github.com/jessekempf/mister-kafka/core"
	"github.com/segmentio/kafka-go"
)

// Consumer is a Kafka consumer that consumes Ts.
type Consumer[K any, V any] struct {
	topic  core.ConsumerTopic[K, V]
	reader *kafka.Reader
}

// NewConsumer creates Consumer[T]s from a broker address, consumer group, topic, and decoder.
func NewConsumer[K any, V any](broker net.Addr, consumerGroup string, topic core.ConsumerTopic[K, V]) *Consumer[K, V] {
	return &Consumer[K, V]{
		topic: topic,
		reader: kafka.NewReader(
			kafka.ReaderConfig{
				Brokers:               []string{broker.String()},
				GroupID:               consumerGroup,
				Topic:                 topic.Name(),
				CommitInterval:        0, // Synchronous
				WatchPartitionChanges: true,
				StartOffset:           kafka.FirstOffset,
				MaxAttempts:           0,
			},
		),
	}
}

// Consume consumes messages containing Ts, passing each to the provided handle callback. Runs in the
// caller's thread. Returns on first error.
func (c *Consumer[K, V]) Consume(ctx context.Context, handle func(*core.InboundMessage[K, V]) error) error {
	for {
		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			return err
		}

		decoded, err := c.topic.DecodeMessage(&msg)

		if err != nil {
			return err
		}

		if err = handle(decoded); err != nil {
			return err
		}

		c.reader.CommitMessages(ctx, msg)
	}
}

// Close closes the consumer and frees associated resources.
func (c *Consumer[K, V]) Close() error {
	return c.reader.Close()
}
