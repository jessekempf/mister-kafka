package planner_test

import (
	"slices"
	"testing"

	"github.com/jessekempf/mister-kafka/consumer/planner"
	"github.com/jessekempf/mister-kafka/core"
	"github.com/stretchr/testify/assert"
)

func TestPartitionPlanner(t *testing.T) {
	t.Run("test empty plan", func(t *testing.T) {
		plan, _ := planner.PartitionPlanner([]*core.InboundMessage[string]{})

		assert.Empty(t, plan.InParallel())
	})

	t.Run("test single-message plan", func(t *testing.T) {
		plan, _ := planner.PartitionPlanner([]*core.InboundMessage[string]{
			{
				Topic:     "t",
				Partition: 0,
				Offset:    0,
				Body:      "oh wow",
			},
		})

		assert.Equal(t, plan.InParallel()[0].InOrder(), []*core.InboundMessage[string]{
			{
				Topic:     "t",
				Partition: 0,
				Offset:    0,
				Body:      "oh wow",
			},
		})
	})

	t.Run("test multi-message plan", func(t *testing.T) {
		p0 := []*core.InboundMessage[string]{
			{
				Topic:     "t",
				Partition: 0,
				Offset:    0,
				Body:      "first",
			},
			{
				Topic:     "t",
				Partition: 0,
				Offset:    1,
				Body:      "second",
			},
		}

		p1 := []*core.InboundMessage[string]{
			{
				Topic:     "t",
				Partition: 1,
				Offset:    0,
				Body:      "third",
			},
			{
				Topic:     "t",
				Partition: 1,
				Offset:    1,
				Body:      "fourth",
			},
		}
		plan, _ := planner.PartitionPlanner(slices.Concat(p0, p1))

		assert.Equal(t, p0, plan.InParallel()[0].InOrder())
		assert.Equal(t, p1, plan.InParallel()[1].InOrder())
	})

	t.Run("test out of order message input", func(t *testing.T) {
		plan, err := planner.PartitionPlanner([]*core.InboundMessage[string]{
			{
				Topic:     "t",
				Partition: 0,
				Offset:    1,
				Body:      "oh wow",
			},
			{
				Topic:     "t",
				Partition: 0,
				Offset:    0,
				Body:      "oh no",
			},
		})

		assert.Nil(t, plan)
		assert.EqualError(t, err, "planner error: received offset 0 for t.0 after offset 1")
	})
}
