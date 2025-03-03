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

func (cr *SyncedCoordinatedReader) FetchMessages(ctx context.Context) ([]*kafka.Message, error) {
	fmreq := &kafka.FetchRequestMulti{
		TopicPartitionOffset: cr.offsets,
		MinBytes:             1,
		MaxBytes:             1024 * 1024,
		MaxWait:              2 * time.Second,
		IsolationLevel:       kafka.ReadCommitted,
	}

	fmresp, err := cr.coordinator.FetchMulti(ctx, fmreq)

	if err != nil {
		return nil, err
	}

	if fmresp.Error != nil {
		return nil, fmresp.Error
	}

	return EnhancedMultiFetchResponse(*fmresp).ReadMessages()
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
