package internal

import (
	"io"

	"github.com/segmentio/kafka-go"
)

type EnhancedMultiFetchResponse kafka.FetchResponseMulti

func (r EnhancedMultiFetchResponse) ReadMessages() ([]*kafka.Message, error) {
	messages := make([]*kafka.Message, 0, 128)

	for topic, contents := range r.Records {
		for partition, records := range contents {
			if r.Errors[topic][partition] != nil {
				return nil, r.Errors[topic][partition]
			}

			for {
				rec, err := records.ReadRecord()

				if err == io.EOF {
					break
				}

				if err != nil {
					return nil, err
				}

				key, err := io.ReadAll(rec.Key)

				if err != nil {
					return nil, err
				}

				val, err := io.ReadAll(rec.Value)

				if err != nil {
					return nil, err
				}

				messages = append(messages, &kafka.Message{
					Topic:         topic,
					Partition:     partition,
					Offset:        rec.Offset,
					HighWaterMark: r.HighWatermark[topic][partition],
					Key:           key,
					Value:         val,
					Headers:       rec.Headers,
					WriterData:    nil,
					Time:          rec.Time,
				})
			}
		}
	}

	return messages, nil
}
