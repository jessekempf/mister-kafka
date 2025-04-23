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
	Key           []byte
	Headers       []Header
	Body          T
	Time          time.Time
}

type Header struct {
	Key   string
	Value []byte
}

type Headers []Header

func (ihs Headers) Get(key string) ([]byte, bool) {
	for _, ih := range ihs {
		if ih.Key == key {
			return ih.Value, true
		}
	}

	return nil, false
}
