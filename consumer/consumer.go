package consumer

import (
	"context"
	"errors"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/jessekempf/mister-kafka/consumer/internal"
	"github.com/jessekempf/mister-kafka/core"
	"github.com/segmentio/kafka-go"
)

// Consumer is a Kafka consumer that consumes Ts.
type Consumer[T any] struct {
	topic   core.ConsumerTopic[T]
	groupID string
	client  *kafka.Client
}

// NewConsumer creates Consumer[T]s from a broker address, consumer group, topic, and decoder.
func NewConsumer[T any](broker net.Addr, consumerGroup string, topic core.ConsumerTopic[T]) *Consumer[T] {
	return &Consumer[T]{
		topic: topic,
		client: &kafka.Client{
			Addr: broker,
		},
		groupID: consumerGroup,
	}
}

// Consume consumes messages containing Ts, passing each to the provided handle callback. Runs in the
// caller's thread. Returns on first error.
func (c *Consumer[T]) Consume(ctx context.Context, handle func(*core.InboundMessage[T]) error) error {
	var initial *internal.InitialCoordinatedReader
	var joined *internal.JoinedCoordinatedReader
	var synced *internal.SyncedCoordinatedReader

	initialize := func() (err error) {
		initial, err = internal.NewCoordinatedReader(ctx, c.client.Addr, c.groupID)
		return
	}

	join := func() (err error) {
		joined, err = initial.JoinGroup(ctx, []string{c.topic.Name()})
		return
	}

	sync := func() (err error) {
		synced, err = joined.SyncGroup(ctx)
		return
	}

	// Arrange to trap SIGNINT
	sc := make(chan os.Signal, 1)
	signal.Notify(sc, syscall.SIGINT)

	for {
		if initial == nil {
			if err := initialize(); err != nil {
				return err
			}
		}

		if joined == nil {
			if err := join(); err != nil {
				return err
			}

			log.Printf("successfully joined group %s for %s\n", c.groupID, c.topic.Name())
		}

		if synced == nil {
			if err := sync(); err != nil {
				return err
			}

			log.Printf("successfully synced group %s for %s\n", c.groupID, c.topic.Name())
		}

		select {
		case sig := <-sc:
			log.Printf("received %s signal, shutting down...", sig)

			return synced.LeaveGroup(ctx)
		default:
			msgs, err := synced.FetchMessages(ctx)

			if err != nil {
				return err
			}

			if len(msgs) == 0 {
				err = synced.Heartbeat(ctx)
			} else {
				messages := make([]*core.InboundMessage[T], 0, len(msgs))

				for _, message := range msgs {
					im, err := c.topic.DecodeMessage(message)

					if err != nil {
						return err
					}

					messages = append(messages, im)
				}

				for _, message := range messages {
					err := handle(message)
					if err != nil {
						return err
					}
				}

				err = synced.CommitMessages(ctx, msgs)
			}

			if err != nil {
				if errors.Is(err, kafka.RebalanceInProgress) {
					log.Printf("rebalance in progress, forcing rejoin")
					joined = nil
					synced = nil
					continue
				}

				return err
			}
		}
	}
}
