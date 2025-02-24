package core

import (
	"time"

	"github.com/segmentio/kafka-go"
)

// OutboundMessage represents a message being produced to Kafka.
type OutboundMessage[K any, V any] interface {
	Key() K
	Value() V
	Headers() []kafka.Header
}

// InboundMessage represents a message being consumed from Kafka.
type InboundMessage[K any, V any] struct {
	Topic         string
	Partition     int
	Offset        int64
	HighWaterMark int64
	Key           K
	Value         V
	Headers       []kafka.Header
	Time          time.Time
}
