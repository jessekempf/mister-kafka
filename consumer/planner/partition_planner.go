package planner

import (
	"github.com/jessekempf/mister-kafka/core"
)

// PartitionPlanner builds an execution plan to process messages in the natural Kafka order. That is,
// partitions are handled simultaneously, while messages within a partition are handled sequentially.
func PartitionPlanner[T any](messages []*core.InboundMessage[T]) (*ExecutionPlan[T], error) {
	type topic_partition struct {
		topic     string
		partition int
	}

	return ProjectionPlanner(func(im *core.InboundMessage[T]) (topic_partition, error) {
		return topic_partition{
			topic:     im.Topic,
			partition: im.Partition,
		}, nil
	})(messages)
}
