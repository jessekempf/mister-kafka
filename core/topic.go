package core

import (
	"fmt"

	"github.com/jessekempf/kafka-go"
)

// ConsumerTopic is a topic from which messages with key type K and value type V may be consumed.
type ConsumerTopic[T any] struct {
	name    string
	decoder func(key []byte, value []byte, headers []kafka.Header) (*T, error)
}

func (t *ConsumerTopic[T]) Name() string {
	return t.name
}

// DecodeMessage takes a kafka.Message and turns it into an InboundMessage.
func (t *ConsumerTopic[T]) DecodeMessage(km *kafka.Message) (*InboundMessage[T], error) {
	if t.name != km.Topic {
		panic(fmt.Sprintf("can't happen: somehow received message from '%s' on '%s'!", km.Topic, t.name))
	}

	body, err := t.decoder(km.Key, km.Value, km.Headers)

	if err != nil {
		return nil, err
	}

	headers := make([]Header, len(km.Headers))

	for i, h := range km.Headers {
		headers[i] = Header{
			Key:   h.Key,
			Value: h.Value,
		}
	}

	return &InboundMessage[T]{
		Topic:         km.Topic,
		Partition:     km.Partition,
		Offset:        km.Offset,
		HighWaterMark: km.HighWaterMark,
		Key:           km.Key,
		Headers:       headers,
		Body:          *body,
		Time:          km.Time,
	}, nil
}

// DeclareConsumerTopic creates a ConsumerTopic[K, V], that names a Kafka topic that is expected
// to contain messages with K keys and V bodies.
func DeclareConsumerTopic[T any](name string, decoder func(key []byte, value []byte, headers []kafka.Header) (*T, error)) ConsumerTopic[T] {
	return ConsumerTopic[T]{
		name:    name,
		decoder: decoder,
	}
}

// ProducerTopic is a topic to which messages with key type K and value type V may be produced.
type ProducerTopic[K any, V any] struct {
	name         string
	keyEncoder   func(key K) ([]byte, error)
	valueEncoder func(value V) ([]byte, error)
}

// EncodeMessage takes an OutboundMessage and turns it into a kafka.Message.
func (t *ProducerTopic[K, V]) EncodeMessage(om OutboundMessage[K, V]) (*kafka.Message, error) {
	k, v, h := om.AsMessage()

	ek, err := t.keyEncoder(k)

	if err != nil {
		return nil, err
	}

	ev, err := t.valueEncoder(v)

	if err != nil {
		return nil, err
	}

	return &kafka.Message{
		Topic:   t.name,
		Key:     ek,
		Value:   ev,
		Headers: h,
	}, nil
}

// DeclareProducerTopic creates a ProducerTopic[T], that names a Kafka topic that is expected
// to accept messages with K keys and V bodies.
func DeclareProducerTopic[K any, V any](name string, keyEncoder func(key K) ([]byte, error), valueEncoder func(value V) ([]byte, error)) ProducerTopic[K, V] {
	return ProducerTopic[K, V]{
		name:         name,
		keyEncoder:   keyEncoder,
		valueEncoder: valueEncoder,
	}
}
