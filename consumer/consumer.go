package consumer

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/jessekempf/mister-kafka/consumer/planner"
	"github.com/jessekempf/mister-kafka/core"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl"
)

// Consumer is a Kafka consumer that consumes Ts.
type Consumer[T any] struct {
	topic   core.ConsumerTopic[T]
	planner planner.Planner[T]

	engine engine
}

func NewConsumer[T any](topic core.ConsumerTopic[T], engine engine) *Consumer[T] {
	return &Consumer[T]{
		topic:   topic,
		planner: planner.PartitionPlanner[T],
		engine:  engine,
	}
}

// WithFetchConfig sets the FetchConfig on the consumer, provided it is a Kafka consumer
func WithFetchConfig[T any](fetchConfig FetchConfig[validated]) func(*Consumer[T]) error {
	return func(c *Consumer[T]) error {
		switch c.engine.(type) {
		case *kafkaEngine:
			(c.engine).(*kafkaEngine).fetchConfig = fetchConfig
		default:
			return fmt.Errorf("setting FetchConfig is not supported with a %T engine", c.engine)
		}

		return nil
	}
}

// WithPlanner sets a non-default planner on the Consumer.
func WithPlanner[T any](planner planner.Planner[T]) func(*Consumer[T]) error {
	return func(c *Consumer[T]) error {
		c.planner = planner
		return nil
	}
}

// WithControlChannel provides a channel for the consumer to listen on, for control messages.
func WithControlChannel[T any](controlChan <-chan engineSignal) func(*Consumer[T]) error {
	return func(c *Consumer[T]) error {
		switch c.engine.(type) {
		case *kafkaEngine:
			(c.engine).(*kafkaEngine).control = controlChan
		default:
			return fmt.Errorf("setting ControlChannel is not supported with a %T engine", c.engine)
		}

		return nil
	}
}

// WithSASL sets the SASL provider to use, for Kafka consumers.
func WithSASL[T any](mechanism sasl.Mechanism) func(*Consumer[T]) error {
	return func(c *Consumer[T]) error {
		switch c.engine.(type) {
		case *kafkaEngine:
			(c.engine).(*kafkaEngine).client.Transport = &kafka.Transport{
				SASL: mechanism,
			}
		default:
			return fmt.Errorf("setting SASL mechanism is not supported with a %T engine", c.engine)
		}

		return nil
	}
}

// NewKafkaConsumer creates Consumer[T]s from a broker address, consumer group, topic, and decoder.
//
// Consumers have the following configurables with sane defaults:
//
// - Planner: The Partition Planner is used if a custom one is not otherwise provided.
// - FetchConfig: The default configuration, except for IsolationLevel being set to ReadCommitted, is used.
func NewKafkaConsumer[T any](broker net.Addr, consumerGroup string, topic core.ConsumerTopic[T], consumerOpts ...func(*Consumer[T]) error) *Consumer[T] {
	control := make(chan engineSignal)

	ke := &kafkaEngine{
		control:   control,
		groupID:   consumerGroup,
		topicName: topic.Name(),
		client: &kafka.Client{
			Addr: broker,
		},
		fetchConfig: FetchConfig[validated]{
			MinBytes:          1,
			MaxBytes:          50 * 1024 * 1024,
			PartitionMaxBytes: 1024 * 1024,
			MaxWait:           10 * time.Second,
			IsolationLevel:    kafka.ReadCommitted,
		},
	}

	c := &Consumer[T]{
		topic:   topic,
		planner: planner.PartitionPlanner[T],
		engine:  ke,
	}

	for _, consumerOpt := range consumerOpts {
		if err := consumerOpt(c); err != nil {
			panic(err)
		}
	}

	return c
}

// NewScriptedConsumer creates Consumer[T]s from a collection of Kafka messages and a topic.
func NewScriptedConsumer[T any](script [][]*kafka.Message, topic core.ConsumerTopic[T], consumerOpts ...func(*Consumer[T]) error) *Consumer[T] {
	c := &Consumer[T]{
		topic:   topic,
		planner: planner.PartitionPlanner[T],
		engine: &scriptedEngine{
			script: script,
		},
	}

	for _, consumerOpt := range consumerOpts {
		if err := consumerOpt(c); err != nil {
			panic(err)
		}
	}

	return c
}

// Consume consumes messages containing Ts, passing each to the provided handle callback. Runs in the
// caller's thread. Returns on first error.
func (c *Consumer[T]) Consume(ctx context.Context, handle func(ctx context.Context, message *core.InboundMessage[T]) error) error {
	return c.engine.run(ctx, func(ctx context.Context, messages []*kafka.Message) ([]*kafka.Message, error) {
		var plan *planner.ExecutionPlan[T]
		decodedMessages := make([]*core.InboundMessage[T], 0, len(messages))

		for i, message := range messages {
			im, err := c.topic.DecodeMessage(message)

			if err != nil {
				return nil, err
			}

			decodedMessages[i] = im
		}

		plan, err := c.planner(decodedMessages)

		if err != nil {
			return nil, err
		}

		err = executePlan(ctx, plan, handle)

		if err != nil {
			return nil, err
		}

		return messages, nil
	})
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
