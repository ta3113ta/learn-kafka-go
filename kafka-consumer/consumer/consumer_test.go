package consumer

import (
	"context"
	"sync"
	"testing"

	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

type MockReader struct {
	mock.Mock
}

func newMockReaderWithCancel(cancel context.CancelFunc) *MockReader {
	reader := &MockReader{}
	reader.
		On("ReadMessage", mock.Anything).
		Return(kafka.Message{
			Topic: "topic1",
			Value: []byte("message1"),
		}, nil).
		Once()

	reader.
		On("ReadMessage", mock.Anything).
		Return(kafka.Message{
			Topic: "topic2",
			Value: []byte("message2"),
		}, nil).
		Once()

	// the third call will close the context
	reader.
		On("ReadMessage", mock.Anything).
		Return(kafka.Message{}, nil).
		Run(func(args mock.Arguments) {
			cancel()
		})

	return reader
}

func (m *MockReader) ReadMessage(ctx context.Context) (kafka.Message, error) {
	args := m.Called(ctx)
	return args.Get(0).(kafka.Message), args.Error(1)
}

func (m *MockReader) Close() error {
	return nil
}

type MockReactor struct {
	handlers map[string][]EventHandler
	mu       sync.RWMutex
}

func (m *MockReactor) Register(topic string, handler EventHandler) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.handlers[topic] = append(m.handlers[topic], handler)
}

func (m *MockReactor) Publish(message *kafka.Message) {
	var wg sync.WaitGroup

	m.mu.RLock()
	defer m.mu.RUnlock()

	for _, handler := range m.handlers[message.Topic] {
		wg.Add(1)
		go func(handler EventHandler) {
			defer wg.Done()
			handler(message)
		}(handler)
	}

	wg.Wait()
}

func TestKafkaConsumer_StartConsuming_WithCancel(t *testing.T) {
	defer goleak.VerifyNone(t)

	var (
		topic1Called bool
		topic2Called bool
	)

	ctx, cancel := context.WithCancel(context.Background())

	reader := newMockReaderWithCancel(cancel)
	reactor := &MockReactor{
		handlers: make(map[string][]EventHandler),
	}

	kc := NewKafkaConsumer(
		ctx,
		reader,
		reactor,
	)

	kc.RegisterHandler("topic1", func(_ *kafka.Message) error {
		topic1Called = true
		return nil
	})

	kc.RegisterHandler("topic2", func(_ *kafka.Message) error {
		topic2Called = true
		return nil
	})

	// use a wait group to wait for context cancellation
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		kc.StartConsuming()
	}()

	wg.Wait()

	// Assert
	require.True(t, topic1Called)
	require.True(t, topic2Called)
}

func TestKafkaConsumer_StartConsuming_WriteToDabase(t *testing.T) {
	defer goleak.VerifyNone(t)

	var (
		topic1Called bool
		topic2Called bool
	)

	ctx, cancel := context.WithCancel(context.Background())

	reader := newMockReaderWithCancel(cancel)
	reactor := &MockReactor{
		handlers: make(map[string][]EventHandler),
	}

	kc := NewKafkaConsumer(
		ctx,
		reader,
		reactor,
	)

	kc.RegisterHandler("topic1", func(_ *kafka.Message) error {
		topic1Called = true
		return nil
	})

	kc.RegisterHandler("topic2", func(_ *kafka.Message) error {
		topic2Called = true
		return nil
	})

	// Use a wait group to wait for context cancellation
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		kc.StartConsuming()
	}()

	// Wait for the context to be cancelled
	wg.Wait()

	// Assert
	require.True(t, topic1Called)
	require.True(t, topic2Called)
}
