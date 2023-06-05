package consumer

import (
	"fmt"
	"sync"

	"github.com/segmentio/kafka-go"
)

var _ Reactor = &KafkaReactor{}

// Reactor is a simple pub-sub implementation
type KafkaReactor struct {
	handlers map[string][]EventHandler
	mu       sync.RWMutex
}

func NewReactor() Reactor {
	return &KafkaReactor{
		handlers: make(map[string][]EventHandler),
	}
}

func (kr *KafkaReactor) Register(topic string, handler EventHandler) {
	kr.mu.Lock()
	defer kr.mu.Unlock()

	kr.handlers[topic] = append(kr.handlers[topic], handler)
}

func (kr *KafkaReactor) Publish(message *kafka.Message) {
	kr.mu.RLock()
	defer kr.mu.RUnlock()

	for _, handler := range kr.handlers[message.Topic] {
		go func(handler EventHandler) {
			defer kr.recover()

			if err := handler(message); err != nil {
				kr.handlerError(err)
			}
		}(handler)
	}
}

func (rt *KafkaReactor) handlerError(err error) {
	fmt.Println("Error in handler:", err)
}

func (rt *KafkaReactor) recover() {
	if r := recover(); r != nil {
		fmt.Println("Recovered from panic:", r)
	}
}
