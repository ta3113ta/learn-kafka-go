package consumer

import (
	"context"

	"github.com/segmentio/kafka-go"
)

type Reader interface {
	ReadMessage(ctx context.Context) (kafka.Message, error)
	Close() error
}

type Reactor interface {
	Register(topic string, handler EventHandler)
	Publish(*kafka.Message)
}

type Database interface {
	Save() error
}