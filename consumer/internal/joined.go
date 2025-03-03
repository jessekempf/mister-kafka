package internal

import (
	"context"
	"log"
	"slices"

	"github.com/segmentio/kafka-go"
)

type JoinedCoordinatedReader struct {
	coordinator   *kafka.Client
	stateVector   StateVector
	groupBalancer kafka.GroupBalancer
	groupMembers  []kafka.JoinGroupResponseMember
}

func (cr *JoinedCoordinatedReader) SyncGroup(ctx context.Context) (*SyncedCoordinatedReader, error) {
	assignments := []kafka.SyncGroupRequestAssignment{}

	if len(cr.groupMembers) > 0 {
		groupMembers := make([]kafka.GroupMember, len(cr.groupMembers))
		topics := []string{}

		for i, joinGroupMember := range cr.groupMembers {
			groupMembers[i] = kafka.GroupMember{
				ID:       joinGroupMember.ID,
				Topics:   joinGroupMember.Metadata.Topics,
				UserData: joinGroupMember.Metadata.UserData,
			}

			for _, topic := range groupMembers[i].Topics {
				if !slices.Contains(topics, topic) {
					topics = append(topics, topic)
				}
			}
		}

		mresp, err := cr.coordinator.Metadata(ctx, &kafka.MetadataRequest{
			Topics: topics,
		})

		if err != nil {
			return nil, err
		}

		partitions := []kafka.Partition{}

		for _, topic := range mresp.Topics {
			partitions = slices.Concat(partitions, topic.Partitions)
		}

		for memberId, memberAssignments := range cr.groupBalancer.AssignGroups(groupMembers, partitions) {
			assignments = append(assignments, kafka.SyncGroupRequestAssignment{
				MemberID: memberId,
				Assignment: kafka.GroupProtocolAssignment{
					AssignedPartitions: memberAssignments,
					UserData:           []byte{},
				},
			})

			log.Printf("JoinedCoordinatedReader.SyncGroup(): using %s to assign %s the following partitions: %v\n", cr.groupBalancer.ProtocolName(), memberId, memberAssignments)
		}

	}

	sgresp, err := cr.coordinator.SyncGroup(ctx, &kafka.SyncGroupRequest{
		GroupID:         cr.stateVector.GroupID,
		GenerationID:    cr.stateVector.GenerationID,
		MemberID:        cr.stateVector.MemberID,
		GroupInstanceID: cr.stateVector.GroupInstanceID,
		ProtocolType:    cr.stateVector.ProtocolType,
		ProtocolName:    cr.stateVector.ProtocolName,
		Assignments:     assignments,
	})

	if err != nil {
		return nil, err
	}

	assignmentCount := 0

	for _, partitions := range sgresp.Assignment.AssignedPartitions {
		assignmentCount += len(partitions)
	}

	ofresp, err := cr.coordinator.OffsetFetch(ctx, &kafka.OffsetFetchRequest{
		GroupID: cr.stateVector.GroupID,
		Topics:  sgresp.Assignment.AssignedPartitions,
	})

	if err != nil {
		return nil, err
	}

	if ofresp.Error != nil {
		return nil, err
	}

	offsets := make(map[string]map[int]int64)

	for topic := range ofresp.Topics {
		for _, partitionInfo := range ofresp.Topics[topic] {
			if offsets[topic] == nil {
				offsets[topic] = make(map[int]int64)
			}

			offsets[topic][partitionInfo.Partition] = partitionInfo.CommittedOffset
		}
	}

	log.Printf(
		"JoinedCoordinatedReader.SyncGroup(): received %d partition assignments: %#v\n",
		assignmentCount,
		sgresp.Assignment.AssignedPartitions,
	)

	return &SyncedCoordinatedReader{
		coordinator: &kafka.Client{
			Addr: cr.coordinator.Addr,
		},
		stateVector: StateVector{
			GroupID:         cr.stateVector.GroupID,
			GenerationID:    cr.stateVector.GenerationID,
			MemberID:        cr.stateVector.MemberID,
			ProtocolType:    sgresp.ProtocolType,
			ProtocolName:    sgresp.ProtocolName,
			GroupInstanceID: cr.stateVector.GroupInstanceID,
		},
		offsets:         offsets,
		assignments:     sgresp.Assignment.AssignedPartitions,
		assignmentCount: assignmentCount,
	}, nil
}
