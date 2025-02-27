package consumer

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"slices"
	"syscall"
	"time"

	"github.com/jessekempf/mister-kafka/core"
	"github.com/segmentio/kafka-go"
)

// Consumer is a Kafka consumer that consumes Ts.
type Consumer[T any] struct {
	topic   core.ConsumerTopic[T]
	groupID string
	reader  *kafka.Reader
	client  *kafka.Client
}

// NewConsumer creates Consumer[T]s from a broker address, consumer group, topic, and decoder.
func NewConsumer[T any](broker net.Addr, consumerGroup string, topic core.ConsumerTopic[T]) *Consumer[T] {
	return &Consumer[T]{
		topic: topic,
		reader: kafka.NewReader(
			kafka.ReaderConfig{
				Brokers:               []string{broker.String()},
				GroupID:               consumerGroup,
				Topic:                 topic.Name(),
				CommitInterval:        0, // Synchronous
				WatchPartitionChanges: true,
				StartOffset:           kafka.FirstOffset,
				MaxAttempts:           0,
				Logger: kafka.LoggerFunc(func(format string, args ...interface{}) {
					log.Printf("kafka.Reader: %s", fmt.Sprintf(format, args...))
				}),
				ErrorLogger: kafka.LoggerFunc(func(format string, args ...interface{}) {
					log.Printf("kafka.Reader[err]: %s", fmt.Sprintf(format, args...))
				}),
			},
		),
		client: &kafka.Client{
			Addr: broker,
		},
		groupID: consumerGroup,
	}
}

// Consume consumes messages containing Ts, passing each to the provided handle callback. Runs in the
// caller's thread. Returns on first error.
func (c *Consumer[T]) Consume(ctx context.Context, handle func(*core.InboundMessage[T]) error) error {
	var joined *JoinedCoordinatedReader
	var synced *SyncedCoordinatedReader

	initial, err := NewCoordinatedReader(ctx, c.client.Addr, c.groupID)

	if err != nil {
		return err
	}

	joined, err = initial.JoinGroup(ctx, []string{c.topic.Name()})

	if err != nil {
		return err
	}

	synced, err = joined.SyncGroup(ctx)

	if err != nil {
		return err
	}

	sc := make(chan os.Signal, 1)

	signal.Notify(sc, syscall.SIGINT)

	for {
		select {
		case sig := <-sc:
			log.Printf("received %s signal, shutting down...", sig)

			if err := c.reader.Close(); err != nil {
				log.Printf("closing Kafka session returned: %v\n", err)
			}

			return synced.LeaveGroup(ctx)
		default:
			timeoutCtx, timeoutCancel := context.WithTimeout(ctx, time.Second)

			defer timeoutCancel()

			msg, err := c.reader.FetchMessage(timeoutCtx)
			ok := true

			if err != nil {
				if err == context.DeadlineExceeded {
					ok = false
				} else {
					return err
				}
			}

			if ok {
				decoded, err := c.topic.DecodeMessage(&msg)

				if err != nil {
					return err
				}

				if err = handle(decoded); err != nil {
					return err
				}

				err = c.reader.CommitMessages(ctx, msg)
				if err != nil {
					return err
				}
			}

			msgs, err := synced.FetchMessages(ctx)

			if err != nil {
				return err
			}

			messages := make([]*core.InboundMessage[T], 0, len(msgs))

			for _, message := range msgs {
				im, err := c.topic.DecodeMessage(message)

				if err != nil {
					return err
				}

				messages = append(messages, im)
			}

			for _, message := range messages {
				err := handle(message)
				if err != nil {
					return err
				}
			}

			err = synced.CommitMessages(ctx, msgs)

			if err != nil {
				if errors.Is(err, kafka.RebalanceInProgress) {
					log.Printf("rebalance in progress, rejoining group")

					joined, err = initial.JoinGroup(ctx, []string{c.topic.Name()})
					if err != nil {
						return err
					}

					synced, err = joined.SyncGroup(ctx)
					if err != nil {
						return err
					}

					continue
				}
				// if errors.Is(err, kafka.IllegalGeneration) {
				// 	log.Printf("caught illegal generation error, resyncing.")
				// 	syncedReader, err = cr2.SyncGroup(ctx)
				// 	if err != nil {
				// 		return err
				// 	}

				// 	continue
				// }

				return err
			}
		}
	}
}

type InitialCoordinatedReader struct {
	group       string
	coordinator *kafka.Client
}

type JoinedCoordinatedReader struct {
	coordinator *kafka.Client
	stateVector StateVector
}

type SyncedCoordinatedReader struct {
	coordinator     *kafka.Client
	stateVector     StateVector
	offsets         map[string]map[int]int64
	assignments     map[string][]int
	assignmentCount int
}

type StateVector struct {
	GroupID         string
	GenerationID    int
	MemberID        string
	GroupInstanceID string
	ProtocolType    string
	ProtocolName    string
}

func NewCoordinatedReader(ctx context.Context, bootstrap net.Addr, group string) (*InitialCoordinatedReader, error) {
	client := &kafka.Client{Addr: bootstrap}

	resp, err := client.FindCoordinator(ctx, &kafka.FindCoordinatorRequest{
		Key:     group,
		KeyType: kafka.CoordinatorKeyTypeConsumer,
	})

	if err != nil {
		return nil, err
	}

	if resp.Error != nil {
		return nil, resp.Error
	}

	addr, err := net.ResolveTCPAddr("tcp", net.JoinHostPort(resp.Coordinator.Host, fmt.Sprintf("%d", resp.Coordinator.Port)))

	if err != nil {
		return nil, err
	}

	return &InitialCoordinatedReader{
		group:       group,
		coordinator: &kafka.Client{Addr: addr},
	}, nil
}

func (cr *InitialCoordinatedReader) JoinGroup(ctx context.Context, topics []string, groupBalancers ...kafka.GroupBalancer) (*JoinedCoordinatedReader, error) {
	hostname, err := os.Hostname()

	if err != nil {
		return nil, err
	}

	if len(groupBalancers) == 0 {
		groupBalancers = []kafka.GroupBalancer{
			kafka.RangeGroupBalancer{},
			kafka.RoundRobinGroupBalancer{},
		}
	}

	groupProtocols := []kafka.GroupProtocol{}

	for _, balancer := range groupBalancers {
		userData, err := balancer.UserData()

		if err != nil {
			return nil, fmt.Errorf("unable to construct protocol metadata for member, %v: %v", balancer.ProtocolName(), err)
		}

		groupProtocols = append(groupProtocols, kafka.GroupProtocol{
			Name: balancer.ProtocolName(),
			Metadata: kafka.GroupProtocolSubscription{
				Topics:   topics,
				UserData: userData,
			},
		},
		)
	}

	req := &kafka.JoinGroupRequest{
		GroupID:          cr.group,
		SessionTimeout:   30 * time.Second,
		RebalanceTimeout: 30 * time.Second,
		GroupInstanceID:  fmt.Sprintf("%s-%d", hostname, os.Getpid()),
		Protocols:        groupProtocols,
		MemberID:         "",
		ProtocolType:     "consumer",
	}

	resp, err := cr.coordinator.JoinGroup(ctx, req)

	if err != nil {
		return nil, err
	}

	if resp.Error != nil {
		return nil, resp.Error
	}

	return &JoinedCoordinatedReader{
		coordinator: &kafka.Client{
			Addr: cr.coordinator.Addr,
		},
		stateVector: StateVector{
			GroupID:         req.GroupID,
			GenerationID:    resp.GenerationID,
			MemberID:        resp.MemberID,
			GroupInstanceID: req.GroupInstanceID,
			ProtocolType:    resp.ProtocolType,
			ProtocolName:    resp.ProtocolName,
		},
	}, nil
}

func (cr *JoinedCoordinatedReader) SyncGroup(ctx context.Context) (*SyncedCoordinatedReader, error) {
	sgresp, err := cr.coordinator.SyncGroup(ctx, &kafka.SyncGroupRequest{
		GroupID:         cr.stateVector.GroupID,
		GenerationID:    cr.stateVector.GenerationID,
		MemberID:        cr.stateVector.MemberID,
		GroupInstanceID: cr.stateVector.GroupInstanceID,
		ProtocolType:    cr.stateVector.ProtocolType,
		ProtocolName:    cr.stateVector.ProtocolName,
	})

	if err != nil {
		return nil, err
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
	assignmentCount := 0

	for topic := range ofresp.Topics {
		for _, partitionInfo := range ofresp.Topics[topic] {
			if offsets[topic] == nil {
				offsets[topic] = make(map[int]int64)
			}

			offsets[topic][partitionInfo.Partition] = partitionInfo.CommittedOffset
			assignmentCount++
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

func (cr *SyncedCoordinatedReader) FetchMessages(ctx context.Context) ([]*kafka.Message, error) {
	messages := make([][]*kafka.Message, 0, cr.assignmentCount)

	for topic, partitions := range cr.assignments {
		for _, partition := range partitions {
			fresp, err := cr.coordinator.Fetch(ctx, &kafka.FetchRequest{
				Topic:          topic,
				Partition:      partition,
				Offset:         cr.offsets[topic][partition],
				MinBytes:       0,
				MaxBytes:       1024 * 1024,
				MaxWait:        1 * time.Second,
				IsolationLevel: kafka.ReadCommitted,
			})

			if err != nil {
				return nil, err
			}

			if fresp.Error != nil {
				return nil, err
			}

			efr := EnhancedFetchResponse{fresp}

			contents, err := efr.ReadMessages()

			if err != nil {
				return nil, err
			}

			messages = append(messages, contents)
		}
	}

	return slices.Concat(messages...), nil
}

type EnhancedFetchResponse struct {
	*kafka.FetchResponse
}

func (r *EnhancedFetchResponse) ReadMessages() ([]*kafka.Message, error) {
	messages := make([]*kafka.Message, 0, 128)

	for {
		rec, err := r.Records.ReadRecord()

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
			Topic:         r.Topic,
			Partition:     r.Partition,
			Offset:        rec.Offset,
			HighWaterMark: r.HighWatermark,
			Key:           key,
			Value:         val,
			Headers:       rec.Headers,
			WriterData:    nil,
			Time:          rec.Time,
		})
	}

	return messages, nil
}

func (cr *SyncedCoordinatedReader) CommitMessages(ctx context.Context, messages []*kafka.Message) error {
	newOffsets := make(map[string]map[int]int64, len(cr.offsets))

	for _, message := range messages {
		if newOffsets[message.Topic] == nil {
			newOffsets[message.Topic] = make(map[int]int64, len(cr.offsets[message.Topic]))
		}

		newOffsets[message.Topic][message.Partition] = message.Offset + 1

	}

	if len(newOffsets) > 0 {
		err := cr.OffsetCommit(ctx, newOffsets)

		if err != nil {
			return err
		}

		for topic, partitionOffsets := range newOffsets {
			for partition, offset := range partitionOffsets {
				cr.offsets[topic][partition] = offset
			}
		}
		return nil
	} else {
		return cr.Heartbeat(ctx)
	}
}

func (cr *SyncedCoordinatedReader) OffsetCommit(ctx context.Context, newOffsets map[string]map[int]int64) error {
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
