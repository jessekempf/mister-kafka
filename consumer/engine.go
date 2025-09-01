package consumer

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/jessekempf/kafka-go"
)

type engine interface {
	run(ctx context.Context, handle func(ctx context.Context, messages []*kafka.Message) ([]*kafka.Message, error)) error
}

type engineSignal int

const (
	engineSignalStop engineSignal = iota
)

func StopOnSignals(signals []syscall.Signal) chan engineSignal {
	signalChannel := make(chan os.Signal, 1)
	engineChannel := make(chan engineSignal, 1)

	signal.Notify(signalChannel, syscall.SIGINT)

	go func() {
		for {
			<-signalChannel
			engineChannel <- engineSignalStop
		}
	}()

	return engineChannel
}

type kafkaEngine struct {
	control         <-chan engineSignal
	groupID         string
	topicName       string
	client          *kafka.Client
	fetchConfig     FetchConfig[validated]
	onMissingOffset func(int) kafka.OffsetRequest
}

func (ke *kafkaEngine) run(ctx context.Context, handle func(ctx context.Context, messages []*kafka.Message) ([]*kafka.Message, error)) error {
	var initial *initialCoordinatedReader
	var joined *joinedCoordinatedReader
	var synced *syncedCoordinatedReader

	initialize := func() (err error) {
		initial, err = newCoordinatedReader(ctx, ke.client, ke.groupID)
		return
	}

	join := func() (err error) {
		joined, err = initial.JoinGroup(ctx, []string{ke.topicName})
		return
	}

	resync := func() (err error) {
		synced, err = joined.SyncGroup(ctx, ke.onMissingOffset)
		return
	}

	for {
		if initial == nil {
			if err := initialize(); err != nil {
				return fmt.Errorf("error initializing consumer: %w", err)
			}
		}

		if joined == nil {
			if err := join(); err != nil {
				return fmt.Errorf("error joining group: %w", err)
			}

			log.Printf("successfully joined group %s for %s\n", ke.groupID, ke.topicName)
		}

		if synced == nil {
			if err := resync(); err != nil {
				return fmt.Errorf("error synchronizing group: %w", err)
			}

			log.Printf("successfully synced group %s for %s\n", ke.groupID, ke.topicName)

			defer func() {
				err := synced.LeaveGroup(ctx)
				if err != nil {
					log.Printf("error on leaving group: %s", err)
				}
			}()
		}

		select {
		case sig := <-ke.control:
			switch sig {
			case engineSignalStop:
				log.Println("received stop signal, shutting down...")
				return nil
			}
		default:
			msgs, err := synced.FetchMessages(ctx, ke.fetchConfig)

			if err != nil {
				return fmt.Errorf("fetch error: %w", err)
			}

			if len(msgs) == 0 {
				err = synced.Heartbeat(ctx)
			} else {
				var handledMessages []*kafka.Message
				handledMessages, err = handle(ctx, msgs)

				if err != nil {
					return err
				}

				err = synced.CommitMessages(ctx, handledMessages)
			}

			if err != nil {
				if errors.Is(err, kafka.RebalanceInProgress) {
					log.Printf("rebalance in progress, forcing rejoin")
					joined = nil
					synced = nil
					continue
				}

				return err
			}
		}
	}
}

type scriptedEngine struct {
	script [][]*kafka.Message
}

func (se *scriptedEngine) run(ctx context.Context, handle func(ctx context.Context, messages []*kafka.Message) ([]*kafka.Message, error)) error {
	for _, fetched := range se.script {
		_, err := handle(ctx, fetched)

		if err != nil {
			return err
		}
	}

	return nil
}
