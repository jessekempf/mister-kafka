package internal

import (
	"context"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

type SyncedCoordinatedReader struct {
	coordinator     *kafka.Client
	stateVector     StateVector
	offsets         map[string]map[int]int64
	assignments     map[string][]int
	assignmentCount int
}

type FetchConfig struct {
	MinBytes          int32
	MaxBytes          int32
	PartitionMaxBytes int32
	MaxWait           time.Duration
	IsolationLevel    kafka.IsolationLevel
}

func (cr *SyncedCoordinatedReader) FetchMessages(ctx context.Context, cfg FetchConfig) ([]*kafka.Message, error) {
	topics := make([]kafka.FetchRequestTopic, 0, len(cr.offsets))

	for topic, partitions := range cr.offsets {
		reqPartitions := make([]kafka.FetchRequestPartition, 0, len(partitions))

		for partition, offset := range partitions {
			reqPartitions = append(reqPartitions, kafka.FetchRequestPartition{
				Partition: partition,
				Offset:    offset,
				MaxBytes:  cfg.PartitionMaxBytes,
			})
		}

		topics = append(topics, kafka.FetchRequestTopic{
			Topic:      topic,
			Partitions: reqPartitions,
		})
	}

	fmreq := &kafka.FetchRequest{
		MinBytes:       cfg.MinBytes,
		MaxBytes:       cfg.MaxBytes,
		MaxWait:        cfg.MaxWait,
		IsolationLevel: kafka.ReadCommitted,
		Topics:         topics,
	}

	fmresp, err := cr.coordinator.Fetch(ctx, fmreq)

	if err != nil {
		return nil, err
	}

	if fmresp.Error != nil {
		return nil, fmresp.Error
	}

	return EnhancedFetchResponse(*fmresp).ReadMessages(cr.offsets)
}

func (cr *SyncedCoordinatedReader) CommitMessages(ctx context.Context, messages []*kafka.Message) error {
	newOffsets := make(map[string]map[int]int64, len(cr.offsets))

	for _, message := range messages {
		if newOffsets[message.Topic] == nil {
			newOffsets[message.Topic] = make(map[int]int64, len(cr.offsets[message.Topic]))
		}

		newOffsets[message.Topic][message.Partition] = message.Offset + 1

	}

	offsetsToCommit := make(map[string][]kafka.OffsetCommit)

	for topic, partitionOffsets := range newOffsets {
		for partition, offset := range partitionOffsets {
			if offset != cr.offsets[topic][partition] {
				offsetsToCommit[topic] = append(offsetsToCommit[topic], kafka.OffsetCommit{
					Partition: partition,
					Offset:    offset,
				})

			}
		}
	}

	ocreq := &kafka.OffsetCommitRequest{
		GroupID:      cr.stateVector.GroupID,
		GenerationID: cr.stateVector.GenerationID,
		MemberID:     cr.stateVector.MemberID,
		InstanceID:   cr.stateVector.GroupInstanceID,
		Topics:       offsetsToCommit,
	}

	ocresp, err := cr.coordinator.OffsetCommit(ctx, ocreq)

	if err != nil {
		return err
	}

	for topic, partitionCommits := range ocresp.Topics {
		for _, offsetCommit := range partitionCommits {
			if offsetCommit.Error != nil {
				return offsetCommit.Error
			}

			log.Printf(
				"committed offset %d for %s %s/%d\n",
				newOffsets[topic][offsetCommit.Partition],
				cr.stateVector.MemberID,
				topic,
				offsetCommit.Partition,
			)

			cr.offsets[topic][offsetCommit.Partition] = newOffsets[topic][offsetCommit.Partition]
		}
	}

	return nil
}

func (cr *SyncedCoordinatedReader) Heartbeat(ctx context.Context) error {
	hbreq := &kafka.HeartbeatRequest{
		GroupID:         cr.stateVector.GroupID,
		GenerationID:    int32(cr.stateVector.GenerationID),
		MemberID:        cr.stateVector.MemberID,
		GroupInstanceID: cr.stateVector.GroupInstanceID,
	}

	hbresp, err := cr.coordinator.Heartbeat(ctx, hbreq)

	if err != nil {
		return err
	}

	log.Printf(
		"SyncedCoordinatedReader.Heartbeat(): sent heartbeat on %s (GenID: %d; MemberID: %s; InstanceID: %s)\n",
		hbreq.GroupID,
		hbreq.GenerationID,
		hbreq.MemberID,
		hbreq.GroupInstanceID,
	)

	return hbresp.Error
}

func (cr *SyncedCoordinatedReader) LeaveGroup(ctx context.Context) error {
	lgresp, err := cr.coordinator.LeaveGroup(ctx, &kafka.LeaveGroupRequest{
		GroupID: cr.stateVector.GroupID,
		Members: []kafka.LeaveGroupRequestMember{
			{
				ID:              cr.stateVector.MemberID,
				GroupInstanceID: cr.stateVector.GroupInstanceID,
			},
		},
	})

	if err != nil {
		return err
	}

	return lgresp.Error
}
