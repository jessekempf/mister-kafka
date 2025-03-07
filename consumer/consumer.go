package consumer

import (
	"context"
	"errors"
	"log"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/jessekempf/mister-kafka/consumer/planner"
	"github.com/jessekempf/mister-kafka/core"
	"github.com/segmentio/kafka-go"
)

// Consumer is a Kafka consumer that consumes Ts.
type Consumer[T any] struct {
	topic       core.ConsumerTopic[T]
	groupID     string
	client      *kafka.Client
	planner     planner.Planner[T]
	fetchConfig FetchConfig[validated]
}

// NewConsumer creates Consumer[T]s from a broker address, consumer group, topic, and decoder.
//
// Consumers have the following configurables with sane defaults:
//
// - Planner: The Partition Planner is used if a custom one is not otherwise provided.
// - FetchConfig: The default configuration, except for IsolationLevel being set to ReadCommitted, is used.
func NewConsumer[T any](broker net.Addr, consumerGroup string, topic core.ConsumerTopic[T]) *Consumer[T] {
	return &Consumer[T]{
		topic: topic,
		client: &kafka.Client{
			Addr: broker,
		},
		groupID: consumerGroup,
		planner: planner.PartitionPlanner[T],
		fetchConfig: FetchConfig[validated]{
			MinBytes:          1,
			MaxBytes:          50 * 1024 * 1024,
			PartitionMaxBytes: 1024 * 1024,
			MaxWait:           10 * time.Second,
			IsolationLevel:    kafka.ReadCommitted,
		},
	}
}

// WithPlanner returns a new Consumer[T] with a non-default planner set.
func (c *Consumer[T]) WithPlanner(planner planner.Planner[T]) *Consumer[T] {
	return &Consumer[T]{
		topic:       c.topic,
		groupID:     c.groupID,
		client:      c.client,
		planner:     planner,
		fetchConfig: c.fetchConfig,
	}
}

// WithFetchConfig returns a new Consumer[T] with a non-default fetch config set.
func (c *Consumer[T]) WithFetchConfig(fetchConfig FetchConfig[validated]) *Consumer[T] {
	return &Consumer[T]{
		topic:       c.topic,
		groupID:     c.groupID,
		client:      c.client,
		planner:     c.planner,
		fetchConfig: fetchConfig,
	}
}

// Consume consumes messages containing Ts, passing each to the provided handle callback. Runs in the
// caller's thread. Returns on first error.
func (c *Consumer[T]) Consume(ctx context.Context, handle func(ctx context.Context, message *core.InboundMessage[T]) error) error {
	var initial *initialCoordinatedReader
	var joined *joinedCoordinatedReader
	var synced *syncedCoordinatedReader

	initialize := func() (err error) {
		initial, err = newCoordinatedReader(ctx, c.client.Addr, c.groupID)
		return
	}

	join := func() (err error) {
		joined, err = initial.JoinGroup(ctx, []string{c.topic.Name()})
		return
	}

	resync := func() (err error) {
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
			if err := resync(); err != nil {
				return err
			}

			log.Printf("successfully synced group %s for %s\n", c.groupID, c.topic.Name())
		}

		select {
		case sig := <-sc:
			log.Printf("received %s signal, shutting down...", sig)

			return synced.LeaveGroup(ctx)
		default:
			msgs, err := synced.FetchMessages(ctx, c.fetchConfig)

			if err != nil {
				return err
			}

			if len(msgs) == 0 {
				err = synced.Heartbeat(ctx)
			} else {
				var plan *planner.ExecutionPlan[T]
				messages := make([]*core.InboundMessage[T], 0, len(msgs))

				for _, message := range msgs {
					im, err := c.topic.DecodeMessage(message)

					if err != nil {
						return err
					}

					messages = append(messages, im)
				}

				plan, err = c.planner(messages)

				if err != nil {
					return err
				}

				err = executePlan(ctx, plan, handle)

				if err != nil {
					return err
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

func executePlan[T any](ctx context.Context, plan *planner.ExecutionPlan[T], handle func(context.Context, *core.InboundMessage[T]) error) error {
	errChan := make(chan error, len(plan.InParallel()))
	executors := sync.WaitGroup{}

	for _, sequence := range plan.InParallel() {
		executors.Add(1)

		go func() {
			for _, message := range sequence.InOrder() {
				err := handle(ctx, message)
				if err != nil {
					errChan <- err
					break
				}
			}
			executors.Done()
		}()
	}

	executors.Wait()

	errs := make([]error, 0, len(errChan))

	return errors.Join(errs...)
}
