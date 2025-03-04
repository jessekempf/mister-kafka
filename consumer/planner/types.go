package planner

import "github.com/jessekempf/mister-kafka/core"

// An ExecutionPlan represents a collection of messages to be passed to a handler function,
// and in which order (if any).
//
// Each plan consists of zero or more Sequence[T]s, and each Sequence[T] consists of of zero or more
// messages. The rule is there is no deterministic ordering of messages _across_ Sequence[T]s, but there
// is strict sequential ordering _within_ a Sequence[T].
type ExecutionPlan[T any] struct {
	parallel []*Sequence[T]
}

// NewExecutionPlan creates a new execution plan with zero sequences.
func NewExecutionPlan[T any]() *ExecutionPlan[T] {
	return &ExecutionPlan[T]{
		parallel: []*Sequence[T]{},
	}
}

// Attach adds a Sequence[T] of messages to the ExecutionPlan.
func (e *ExecutionPlan[T]) Attach(s *Sequence[T]) {
	e.parallel = append(e.parallel, s)
}

// InParallel retrieves the Sequence[T]s that make up this ExecutionPlan.
func (e ExecutionPlan[T]) InParallel() []*Sequence[T] {
	return e.parallel
}

// Count gets the number of different Sequence[T]s that make up this ExecutionPlan.
func (e ExecutionPlan[T]) Count() int {
	return len(e.parallel)
}

// A Sequence represents a collection of messages to be passed to a handler function in order (hence the name).
type Sequence[T any] struct {
	messages []*core.InboundMessage[T]
}

// EmptySequence creates a new, and empty, sequence.
func EmptySequence[T any]() *Sequence[T] {
	return &Sequence[T]{
		messages: []*core.InboundMessage[T]{},
	}
}

// Append adds a message to the end of the sequence.
func (s *Sequence[T]) Append(x *core.InboundMessage[T]) {
	s.messages = append(s.messages, x)
}

// InOrder produces a list of messages in order.
func (s Sequence[T]) InOrder() []*core.InboundMessage[T] {
	return s.messages
}

// A Planner is a function that turns a bunch of messages into either an ExecutionPlan or an error.
type Planner[T any] func(messages []*core.InboundMessage[T]) (*ExecutionPlan[T], error)
