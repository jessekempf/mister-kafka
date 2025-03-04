package planner

import (
	"fmt"

	"github.com/jessekempf/mister-kafka/core"
)

func PartitionPlanner[T any](messages []*core.InboundMessage[T]) (*ExecutionPlan[T], error) {
	type partition struct {
		lastOffset int64
		sequence   *Sequence[T]
	}

	partitions := make(map[string]map[int]*partition)

	for _, message := range messages {
		if partitions[message.Topic] == nil {
			partitions[message.Topic] = make(map[int]*partition)
		}

		if _, ok := partitions[message.Topic][message.Partition]; !ok {
			partitions[message.Topic][message.Partition] = &partition{
				lastOffset: message.Offset - 1,
				sequence:   EmptySequence[T](),
			}
		}

		p := partitions[message.Topic][message.Partition]

		if !(p.lastOffset < message.Offset) {
			return nil, fmt.Errorf("planner error: received offset %d for %s.%d after offset %d", message.Offset, message.Topic, message.Partition, p.lastOffset)
		}

		p.lastOffset = message.Offset
		p.sequence.Append(message)
	}

	plan := NewExecutionPlan[T]()

	for _, inner := range partitions {
		for _, p := range inner {
			plan.Attach(p.sequence)
		}
	}

	return plan, nil
}
