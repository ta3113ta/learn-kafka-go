package consumer

import (
	"sync"
	"sync/atomic"
	"testing"

	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"go.uber.org/goleak"
)

// Define a dummy handler function for benchmarking
func dummyHandler(message *kafka.Message) error {
	return nil
}

type Counter struct {
	countA int32
	countB int32
}

const topic = "test-topic"

func TestKafkaReactor_Register(t *testing.T) {
	// comment out if you want to see cpu  profile
	defer goleak.VerifyNone(t)

	c := Counter{}
	sampling := 10

	var wg sync.WaitGroup
	wg.Add(sampling * 2) // 2 handlers

	reactor := NewReactor()
	reactor.Register(topic, func(m *kafka.Message) error {
		defer wg.Done()

		if m.Value[0] == 'A' {
			atomic.AddInt32(&c.countA, 1)
		} else {
			atomic.AddInt32(&c.countB, 1)
		}

		return nil
	})

	for i := 0; i < sampling; i++ {
		reactor.Publish(&kafka.Message{
			Topic: topic,
			Value: []byte("A"),
		})
		reactor.Publish(&kafka.Message{
			Topic: topic,
			Value: []byte("B"),
		})
	}

	wg.Wait()

	assert.Equal(t, int32(10), c.countA)
	assert.Equal(t, int32(10), c.countB)
}

func BenchmarkKafkaReactor(b *testing.B) {
	c := Counter{}

	var wg sync.WaitGroup

	reactor := NewReactor()
	reactor.Register(topic, func(m *kafka.Message) error {
		defer wg.Done()
		if m.Value[0] == 'A' {
			atomic.AddInt32(&c.countA, 1)
		} else {
			atomic.AddInt32(&c.countB, 1)
		}
		return nil
	})

	wg.Add(b.N * 2)
	for i := 0; i < b.N; i++ {
		reactor.Publish(&kafka.Message{
			Topic: topic,
			Value: []byte("A"),
		})
		reactor.Publish(&kafka.Message{
			Topic: topic,
			Value: []byte("B"),
		})
	}

	wg.Wait()
}
