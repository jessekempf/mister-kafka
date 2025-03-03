package internal

import (
	"context"
	"fmt"
	"net"
	"os"
	"time"

	"github.com/segmentio/kafka-go"
)

type InitialCoordinatedReader struct {
	group       string
	coordinator *kafka.Client
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
		groupMembers:  resp.Members,
		groupBalancer: groupBalancer,
	}, nil
}
