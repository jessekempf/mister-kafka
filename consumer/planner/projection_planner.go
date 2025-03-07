package planner

import (
	"fmt"

	"github.com/jessekempf/mister-kafka/core"
)

// ProjectionPlanner is the archetypal and most general Planner. It accepts as its argument a projection function,
// which maps an InboundMessage[T] into an element of P. P, mathematically speaking, is the set of mathematical
// partitions of the possible values of InboundMessage[T]. Two messages that map to the same p ∈ P are still ordered
// by offset.
//
// That's terribly abstract, so some examples should make it clearer. The two different usages of "partition" here
// do not help.
//
// Kafka's own natural plan is that the p ∈ P is a topic+partition, and P is the set of all topic+partitions that
// appear in the collection of input messages. By giving a projection from a message to its topic+partition, we are
// able to build a plan that evaluates topic+partitions concurrently, but messages within a topic+partition sequentially.
//
// Let us imagine we have a T which contains a username, and the rule is that only messages with the same username must
// process sequentually. We could then write a projection from InboundMessage[T] to (topic, partition, username) and see
// even more sequences of messages being evaluated in parallel.
func ProjectionPlanner[T any, P comparable](projection func(*core.InboundMessage[T]) (P, error)) func([]*core.InboundMessage[T]) (*ExecutionPlan[T], error) {
	type grouping struct {
		lastOffset int64
		sequence   *Sequence[T]
	}

	// Note: the correctness of this function is implied by the partition planner tests.
	return func(messages []*core.InboundMessage[T]) (*ExecutionPlan[T], error) {
		parallels := make(map[P]*grouping)

		for _, message := range messages {
			proj, err := projection(message)

			if err != nil {
				return nil, err
			}

			if _, ok := parallels[proj]; !ok {
				parallels[proj] = &grouping{
					lastOffset: message.Offset - 1,
					sequence:   EmptySequence[T](),
				}
			}

			p := parallels[proj]

			if !(p.lastOffset < message.Offset) {
				return nil, fmt.Errorf("planner error: received offset %d for %s.%d after offset %d", message.Offset, message.Topic, message.Partition, p.lastOffset)
			}

			p.lastOffset = message.Offset
			p.sequence.Append(message)
		}

		plan := NewExecutionPlan[T]()

		for _, parallel := range parallels {
			plan.Attach(parallel.sequence)
		}

		return plan, nil
	}
}
