package consumer

import (
	"io"

	"github.com/jessekempf/kafka-go"
)

type enhancedFetchResponse kafka.FetchResponse

func (r enhancedFetchResponse) ReadMessages(offsets map[string]map[int]int64) ([]*kafka.Message, error) {
	messages := make([]*kafka.Message, 0, 128)

	for _, topic := range r.Topics {
		for _, partition := range topic.Partitions {
			if partition.Error != nil {
				return nil, partition.Error
			}

			for {
				rec, err := partition.Records.ReadRecord()

				if err == io.EOF {
					break
				}

				if err != nil {
					return nil, err
				}

				if rec.Offset < offsets[topic.Topic][partition.Partition] {
					continue
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
					Topic:         topic.Topic,
					Partition:     partition.Partition,
					Offset:        rec.Offset,
					HighWaterMark: partition.HighWatermark,
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
