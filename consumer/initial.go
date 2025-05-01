package consumer

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"time"

	"github.com/segmentio/kafka-go"
)

type wrappedKafkaError struct {
	message    string
	underlying kafka.Error
}

func (wke *wrappedKafkaError) Unwrap() error {
	return wke.underlying
}

func (wke *wrappedKafkaError) Error() string {
	return fmt.Sprintf("%s [kafka error %d]", wke.message, wke.underlying)
}

type initialCoordinatedReader struct {
	group       string
	coordinator *kafka.Client
}

func newCoordinatedReader(ctx context.Context, bootstrap *kafka.Client, group string) (*initialCoordinatedReader, error) {
	resp, err := bootstrap.FindCoordinator(ctx, &kafka.FindCoordinatorRequest{
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

	return &initialCoordinatedReader{
		group:       group,
		coordinator: &kafka.Client{Addr: addr, Transport: bootstrap.Transport},
	}, nil
}

func (cr *initialCoordinatedReader) JoinGroup(ctx context.Context, topics []string, groupBalancers ...kafka.GroupBalancer) (*joinedCoordinatedReader, error) {
	hostname, err := os.Hostname()

	if err != nil {
		return nil, err
	}

	metadata, err := cr.coordinator.Metadata(ctx, &kafka.MetadataRequest{
		Topics: topics,
	})

	if err != nil {
		return nil, fmt.Errorf("error requesting cluster metadata: %w", err)
	}

	for _, topicMetadata := range metadata.Topics {
		if topicMetadata.Error != nil {
			if errors.Is(topicMetadata.Error, kafka.UnknownTopicOrPartition) {
				return nil, &wrappedKafkaError{
					message:    fmt.Sprintf("topic %s does not exist", topicMetadata.Name),
					underlying: kafka.UnknownTopicOrPartition,
				}
			}

			return nil, fmt.Errorf("error retrieving topic metadata for %s: %w", topicMetadata.Name, topicMetadata.Error)
		}
	}

	if len(groupBalancers) == 0 {
		groupBalancers = []kafka.GroupBalancer{
			kafka.RangeGroupBalancer{},
			kafka.RoundRobinGroupBalancer{},
		}
	}

	groupProtocols := []kafka.GroupProtocol{}
	groupBalancerMap := make(map[string]kafka.GroupBalancer, len(groupBalancers))

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

		groupBalancerMap[balancer.ProtocolName()] = balancer
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

	groupBalancer, ok := groupBalancerMap[resp.ProtocolName]
	if !ok {
		return nil, fmt.Errorf("coordinator assigned unsupported group balancer '%s'", resp.ProtocolName)
	}

	return &joinedCoordinatedReader{
		coordinator: &kafka.Client{
			Addr:      cr.coordinator.Addr,
			Transport: cr.coordinator.Transport,
		},
		stateVector: stateVector{
			GroupID:         req.GroupID,
			GenerationID:    resp.GenerationID,
			MemberID:        resp.MemberID,
			GroupInstanceID: req.GroupInstanceID,
			ProtocolType:    resp.ProtocolType,
			ProtocolName:    resp.ProtocolName,
		},
		groupMembers:  resp.Members,
		groupBalancer: groupBalancer,
	}, nil
}
