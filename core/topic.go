package core

import (
	"fmt"

	"github.com/segmentio/kafka-go"
)

// ConsumerTopic is a topic from which messages with key type K and value type V may be consumed.
type ConsumerTopic[K any, V any] struct {
	name    string
	decoder func([]byte, []byte) (K, V, error)
}

// DecodeMessage takes a kafka.Message and turns it into an InboundMessage.
func (t *ConsumerTopic[K, V]) DecodeMessage(km *kafka.Message) (*InboundMessage[K, V], error) {
	if t.name != km.Topic {
		panic(fmt.Sprintf("can't happen: somehow received message from %s on %s!", km.Topic, t.name))
	}

	k, v, err := t.decoder(km.Key, km.Value)

	if err != nil {
		return nil, err
	}

	return &InboundMessage[K, V]{
		Topic:         km.Topic,
		Partition:     km.Partition,
		Offset:        km.Offset,
		HighWaterMark: km.HighWaterMark,
		Key:           k,
		Value:         v,
		Headers:       km.Headers,
		Time:          km.Time,
	}, nil
}

// DeclareConsumerTopic creates a ConsumerTopic[K, V], that names a Kafka topic that is expected
// to contain messages with K keys and V bodies.
func DeclareConsumerTopic[K any, V any](name string, decoder func([]byte, []byte) (K, V, error)) ConsumerTopic[K, V] {
	return ConsumerTopic[K, V]{
		name:    name,
		decoder: decoder,
	}
}

// ProducerTopic is a topic to which messages with key type K and value type V may be produced.
type ProducerTopic[K any, V any] struct {
	name    string
	encoder func(K, V) ([]byte, []byte, error)
}

// EncodeMessage takes an OutboundMessage and turns it into a kafka.Message.
func (t *ProducerTopic[K, V]) EncodeMessage(om *OutboundMessage[K, V]) (*kafka.Message, error) {
	k, v, err := t.encoder(om.Key, om.Value)

	if err != nil {
		return nil, err
	}

	return &kafka.Message{
		Key:     k,
		Value:   v,
		Headers: om.Headers,
	}, nil
}

// DeclareProducerTopic creates a ProducerTopic[T], that names a Kafka topic that is expected
// to accept messages with K keys and V bodies.
func DeclareProducerTopic[K any, V any](name string, encoder func(K, V) ([]byte, []byte, error)) ProducerTopic[K, V] {
	return ProducerTopic[K, V]{
		name:    name,
		encoder: encoder,
	}
}
