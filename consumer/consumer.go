package consumer

import (
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/davecgh/go-spew/spew"
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
	dgresp, err := c.client.DescribeGroups(ctx, &kafka.DescribeGroupsRequest{GroupIDs: []string{c.groupID}})

	if err != nil {
		return err
	}

	log.Println(spew.Sdump(dgresp))

	//
	//
	//

	sc := make(chan os.Signal, 1)

	signal.Notify(sc, syscall.SIGINT)

	resp, err := c.client.FindCoordinator(ctx, &kafka.FindCoordinatorRequest{
		Key:     c.groupID,
		KeyType: kafka.CoordinatorKeyTypeConsumer,
	})

	if err != nil {
		return err
	}

	if resp.Error != nil {
		return resp.Error
	}

	hostname, err := os.Hostname()

	if err != nil {
		return err
	}

	groupBalancers := []kafka.GroupBalancer{
		kafka.RoundRobinGroupBalancer{},
	}

	groupProtocols := []kafka.GroupProtocol{}

	for _, balancer := range groupBalancers {
		userData, err := balancer.UserData()

		if err != nil {
			return fmt.Errorf("unable to construct protocol metadata for member, %v: %v", balancer.ProtocolName(), err)
		}

		groupProtocols = append(groupProtocols, kafka.GroupProtocol{
			Name: balancer.ProtocolName(),
			Metadata: kafka.GroupProtocolSubscription{
				Topics:   []string{c.topic.Name()},
				UserData: userData,
			},
		},
		)
	}

	jgreq := &kafka.JoinGroupRequest{
		GroupID:          c.groupID,
		SessionTimeout:   30 * time.Second,
		RebalanceTimeout: 30 * time.Second,
		GroupInstanceID:  hostname,
		Protocols:        groupProtocols,
		Addr:             nil,
		MemberID:         "",
		ProtocolType:     "consumer",
	}

	log.Println(spew.Sdump(jgreq))

	jgresp, err := c.client.JoinGroup(ctx, jgreq)

	if err != nil {
		return err
	}

	if jgresp.Error != nil {
		return jgresp.Error
	}

	log.Printf("%#v\n", jgresp)

	log.Printf("joined %s as member %s (leader? %t)\n", jgreq.GroupID, jgresp.MemberID, jgresp.MemberID == jgresp.LeaderID)

	log.Println(spew.Sdump(jgresp))

	dgresp, err = c.client.DescribeGroups(ctx, &kafka.DescribeGroupsRequest{GroupIDs: []string{c.groupID}})

	if err != nil {
		return err
	}

	log.Println(spew.Sdump(dgresp))

	sgresp, err := c.client.SyncGroup(ctx, &kafka.SyncGroupRequest{
		GroupID:      jgreq.GroupID,
		GenerationID: jgresp.GenerationID,
		MemberID:     jgresp.MemberID,
		ProtocolType: jgresp.ProtocolType,
		ProtocolName: jgresp.ProtocolName,
	})

	if err != nil {
		return err
	}

	log.Println(spew.Sdump(sgresp))

	dgresp, err = c.client.DescribeGroups(ctx, &kafka.DescribeGroupsRequest{GroupIDs: []string{c.groupID}})

	if err != nil {
		return err
	}

	log.Println(spew.Sdump(dgresp))

	for {
		select {
		case sig := <-sc:
			log.Printf("received %s signal, shutting down...", sig)

			if err := c.reader.Close(); err != nil {
				log.Printf("closing Kafka session returned: %v\n", err)
			}

			lgresp, err := c.client.LeaveGroup(ctx, &kafka.LeaveGroupRequest{
				GroupID: jgreq.GroupID,
				Members: []kafka.LeaveGroupRequestMember{
					{
						ID:              jgresp.MemberID,
						GroupInstanceID: jgreq.GroupInstanceID,
					},
				},
			})

			log.Printf("%+v / %+v\n", lgresp, err)

			return nil
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

				c.reader.CommitMessages(ctx, msg)
			}

			messages := make([]*core.InboundMessage[T], 0, 1024)

			ofresp, err := c.client.OffsetFetch(ctx, &kafka.OffsetFetchRequest{
				GroupID: c.groupID,
				Topics:  sgresp.Assignment.AssignedPartitions,
			})

			if err != nil {
				return err
			}

			if ofresp.Error != nil {
				return err
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

			log.Printf("offset table: %#v\n", offsets)

			for _, partition := range sgresp.Assignment.AssignedPartitions[c.topic.Name()] {
				fresp, err := c.client.Fetch(ctx, &kafka.FetchRequest{
					Topic:          c.topic.Name(),
					Partition:      partition,
					Offset:         offsets[c.topic.Name()][partition],
					MinBytes:       0,
					MaxBytes:       0,
					MaxWait:        1 * time.Second,
					IsolationLevel: kafka.ReadCommitted,
				})

				if err != nil {
					return err
				}

				if fresp.Error != nil {
					return err
				}

				for {
					rec, err := fresp.Records.ReadRecord()

					if err == io.EOF {
						break
					}

					if err != nil {
						return err
					}

					var key []byte
					var val []byte

					_, err = rec.Key.Read(key)

					if err != nil {
						return err
					}

					_, err = rec.Value.Read(val)

					if err != nil {
						return err
					}

					decoded, err := c.topic.DecodeMessage(&kafka.Message{
						Topic:         c.topic.Name(),
						Partition:     partition,
						Offset:        rec.Offset,
						HighWaterMark: fresp.HighWatermark,
						Key:           key,
						Value:         val,
						Headers:       rec.Headers,
						WriterData:    nil,
						Time:          rec.Time,
					})

					if err != nil {
						return err
					}

					messages = append(messages, decoded)
				}

				offsetsToCommit := make(map[string][]kafka.OffsetCommit)

				for _, message := range messages {
					err := handle(message)
					if err != nil {
						return err
					}

					offsetsToCommit[message.Topic] = append(offsetsToCommit[message.Topic], kafka.OffsetCommit{
						Partition: message.Partition,
						Offset:    message.Offset + 1,
					})
				}

				ocresp, err := c.client.OffsetCommit(ctx, &kafka.OffsetCommitRequest{
					GroupID:      c.groupID,
					GenerationID: jgresp.GenerationID,
					MemberID:     jgresp.MemberID,
					InstanceID:   jgreq.GroupInstanceID,
					Topics:       offsetsToCommit,
				})

				if err != nil {
					return err
				}

				for _, offsetCommit := range ocresp.Topics[c.topic.Name()] {
					log.Printf("committed offsets for %s, returned %w\n", offsetCommit.Partition, offsetCommit.Error)
				}
			}

			hbreq := &kafka.HeartbeatRequest{
				GroupID:         c.groupID,
				GenerationID:    int32(jgresp.GenerationID),
				MemberID:        jgresp.MemberID,
				GroupInstanceID: jgreq.GroupInstanceID,
			}

			hbresp, err := c.client.Heartbeat(ctx, hbreq)

			if err != nil {
				return err
			}

			if hbresp.Error != nil {
				return hbresp.Error
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
	coordinator *kafka.Client
	stateVector StateVector
	offsets     map[string]map[int]int64
	assignments map[string][]int
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

	addr, err := net.ResolveTCPAddr("tcp", resp.Coordinator.Host)

	if err != nil {
		return nil, err
	}

	addr.Port = resp.Coordinator.Port

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
		GroupInstanceID:  hostname,
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
		coordinator: cr.coordinator,
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
		GroupID:      cr.stateVector.GroupID,
		GenerationID: cr.stateVector.GenerationID,
		MemberID:     cr.stateVector.MemberID,
		ProtocolType: cr.stateVector.ProtocolType,
		ProtocolName: cr.stateVector.ProtocolName,
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

	for topic := range ofresp.Topics {
		for _, partitionInfo := range ofresp.Topics[topic] {
			if offsets[topic] == nil {
				offsets[topic] = make(map[int]int64)
			}

			offsets[topic][partitionInfo.Partition] = partitionInfo.CommittedOffset
		}
	}

	return &SyncedCoordinatedReader{
		coordinator: &kafka.Client{},
		stateVector: StateVector{
			GroupID:      cr.stateVector.GroupID,
			GenerationID: cr.stateVector.GenerationID,
			MemberID:     cr.stateVector.MemberID,
			ProtocolType: sgresp.ProtocolType,
			ProtocolName: sgresp.ProtocolName,
		},
		offsets:     offsets,
		assignments: sgresp.Assignment.AssignedPartitions,
	}, nil
}

func (cr *SyncedCoordinatedReader) FetchMessages(ctx context.Context) ([]*kafka.Message, error) {
	messages := make([]*kafka.Message, 0, len(cr.assignments)*1024)

	for topic, partitions := range cr.assignments {
		for _, partition := range partitions {
			fresp, err := cr.coordinator.Fetch(ctx, &kafka.FetchRequest{
				Topic:          topic,
				Partition:      partition,
				Offset:         cr.offsets[topic][partition],
				MinBytes:       0,
				MaxBytes:       0,
				MaxWait:        1 * time.Second,
				IsolationLevel: kafka.ReadCommitted,
			})

			if err != nil {
				return nil, err
			}

			if fresp.Error != nil {
				return nil, err
			}

			for {
				rec, err := fresp.Records.ReadRecord()

				if err == io.EOF {
					break
				}

				if err != nil {
					return nil, err
				}

				var key []byte
				var val []byte

				_, err = rec.Key.Read(key)

				if err != nil {
					return nil, err
				}

				_, err = rec.Value.Read(val)

				if err != nil {
					return nil, err
				}

				messages = append(messages, &kafka.Message{
					Topic:         topic,
					Partition:     partition,
					Offset:        rec.Offset,
					HighWaterMark: fresp.HighWatermark,
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

func (cr *SyncedCoordinatedReader) CommitMessages(ctx context.Context, messages []*kafka.Message) error {
	if len(messages) == 0 {
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

		if hbresp.Error != nil {
			return hbresp.Error
		}

		return nil
	}

	offsetsToCommit := make(map[string][]kafka.OffsetCommit)

	for _, message := range messages {
		offsetsToCommit[message.Topic] = append(offsetsToCommit[message.Topic], kafka.OffsetCommit{
			Partition: message.Partition,
			Offset:    message.Offset + 1,
		})
	}

	ocresp, err := cr.coordinator.OffsetCommit(ctx, &kafka.OffsetCommitRequest{
		GroupID:      cr.stateVector.GroupID,
		GenerationID: cr.stateVector.GenerationID,
		MemberID:     cr.stateVector.MemberID,
		InstanceID:   cr.stateVector.GroupID,
		Topics:       offsetsToCommit,
	})

	if err != nil {
		return err
	}

	for topic, partitionCommits := range ocresp.Topics {
		for _, offsetCommit := range partitionCommits {
			log.Printf("committed offsets for %s/%s, returned %w\n", topic, offsetCommit.Partition, offsetCommit.Error)
		}
	}

	return nil
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
