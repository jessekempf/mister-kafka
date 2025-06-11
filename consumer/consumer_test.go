package consumer_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/jessekempf/kafka-go"
	"github.com/jessekempf/mister-kafka/consumer"
	"github.com/jessekempf/mister-kafka/core"
	"github.com/stretchr/testify/assert"
)

func TestConsume(t *testing.T) {
	topicFoo := core.DeclareConsumerTopic[string]("foo",
		func(key, value []byte, headers []kafka.Header) (*string, error) {
			str := string(value)
			return &str, nil
		})

	c1 := consumer.NewScriptedConsumer(
		[][]*kafka.Message{
			{
				{
					Topic:     "foo",
					Partition: 0,
					Offset:    0,
					Key:       []byte("hi"),
					Value:     []byte("there"),
				},
			},
		},
		topicFoo,
	)

	c2 := consumer.NewScriptedConsumer(
		[][]*kafka.Message{
			{
				{
					Topic:     "foo",
					Partition: 0,
					Offset:    0,
					Key:       []byte("hi"),
					Value:     []byte("everybody"),
				},
				{
					Topic:     "foo",
					Partition: 1,
					Offset:    0,
					Key:       []byte("bye"),
					Value:     []byte("pancreas"),
				},
			},
		},
		topicFoo,
	)

	t.Run("returns nil if no handler returns nil", func(t *testing.T) {
		err := c2.Consume(context.Background(), func(ctx context.Context, message *core.InboundMessage[string]) error {
			return nil
		})

		assert.NoError(t, err)
	})

	t.Run("returns a single erroring handler's error", func(t *testing.T) {
		err := c1.Consume(context.Background(), func(ctx context.Context, message *core.InboundMessage[string]) error {
			return fmt.Errorf("oh no!")
		})

		assert.Error(t, err, "oh no!")
	})

	t.Run("returns multiple handlers' errors", func(t *testing.T) {
		err := c2.Consume(context.Background(), func(ctx context.Context, message *core.InboundMessage[string]) error {
			return fmt.Errorf("i dislike %s", message.Body)
		})

		assert.Equal(t, err, errors.Join(
			fmt.Errorf("i dislike pancreas"),
			fmt.Errorf("i dislike everybody"),
		))
	})
}
