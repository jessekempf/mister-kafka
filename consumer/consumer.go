package consumer

import (
	"context"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/jessekempf/mister-kafka/core"
	"github.com/segmentio/kafka-go"
)

// Consumer is a Kafka consumer that consumes Ts.
type Consumer[T any] struct {
	topic  core.ConsumerTopic[T]
	reader *kafka.Reader
}

// NewConsumer creates Consumer[T]s from a broker address, consumer group, topic, and decoder.
func NewConsumer[T any](broker net.Addr, consumerGroup string, topic core.ConsumerTopic[T]) *Consumer[T] {
	return &Consumer[T]{
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
func (c *Consumer[T]) Consume(ctx context.Context, handle func(*core.InboundMessage[T]) error) error {
	sc := make(chan os.Signal, 1)

	signal.Notify(sc, syscall.SIGINT)

	for {
		select {
		case sig := <-sc:
			log.Printf("received %s, shutting down", sig)
			c.reader.Close()
			return nil
		default:
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
}
