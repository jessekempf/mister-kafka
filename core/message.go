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

func MapInboundMessage[T any, U any](a *InboundMessage[T], f func(T) (U, error)) (*InboundMessage[U], error) {
	u, err := f(a.Body)

	if err != nil {
		return nil, err
	}

	return &InboundMessage[U]{
		Topic:         a.Topic,
		Partition:     a.Partition,
		Offset:        a.Offset,
		HighWaterMark: a.HighWaterMark,
		Key:           a.Key,
		Headers:       a.Headers,
		Body:          u,
		Time:          a.Time,
	}, nil
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
