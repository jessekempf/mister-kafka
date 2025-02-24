package core

import (
	"time"

	"github.com/segmentio/kafka-go"
)

// OutboundMessage represents a message being produced to Kafka.
type OutboundMessage[K any, V any] interface {
	AsMessage() (K, V, []kafka.Header)
}

// InboundMessage represents a message being consumed from Kafka.
type InboundMessage[T any] struct {
	Topic         string
	Partition     int
	Offset        int64
	HighWaterMark int64
	Body          T
	Time          time.Time
}
