package counter

import (
	"fmt"

	"github.com/segmentio/kafka-go"
)

type Counter struct {
}

func New() *Counter {
	return &Counter{}
}

func (c *Counter) HandleTopicA(message *kafka.Message) error {
	fmt.Printf("Handling topic A: %s, offset: %d\n", string(message.Value), message.Offset)
	return nil
}

func (c *Counter) HandleTopicB(message *kafka.Message) error {
	fmt.Printf("Handling topic B: %s, offset: %d\n", string(message.Value), message.Offset)
	return nil
}
