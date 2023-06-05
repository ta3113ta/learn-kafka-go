package consumer

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/segmentio/kafka-go"
)

type EventHandler func(*kafka.Message) error

type KafkaConsumer struct {
	maxConcurrency uint
	ctx            context.Context
	reader         Reader
	reactor        Reactor
}

func NewKafkaConsumer(ctx context.Context, reader Reader, reactor Reactor) *KafkaConsumer {
	return &KafkaConsumer{
		ctx:     ctx,
		reader:  reader,
		reactor: reactor,
	}
}

func (k *KafkaConsumer) RegisterHandler(topic string, handler EventHandler) {
	k.reactor.Register(topic, handler)
}

func (k *KafkaConsumer) StartConsuming() {
	signals := make(chan os.Signal, 1)

	// SIGINT is sent when user wants to interrupt the process (Ctrl+C)
	// SIGTERM is sent when the system wants the process to terminate
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

	go k.consume()

	fmt.Println("Serving...")

	for {
		select {
		case <-k.ctx.Done():
			return
		case <-signals:
			return
		}
	}
}

func (k *KafkaConsumer) consume() {
	defer k.reader.Close()

	for {
		select {
		case <-k.ctx.Done():
			fmt.Println("context cancelled")
			return
		default:
			m, err := k.reader.ReadMessage(k.ctx)
			if err != nil {
				fmt.Println("error reading message: ", err)
				break
			}

			// TODO: log go here

			k.reactor.Publish(&m)
		}
	}
}
