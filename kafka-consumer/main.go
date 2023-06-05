package main

import (
	"context"

	"learn-kafka/kafka-consumer/consumer"
	"learn-kafka/pkg/counter"

	"github.com/segmentio/kafka-go"
)

type Config struct {
	Brokers []string
	GroupID string
	Topics  []string
}

func newReader(cfg Config) consumer.Reader {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     cfg.Brokers,
		GroupID:     cfg.GroupID,
		GroupTopics: cfg.Topics,
		MinBytes:    10e3, // 10KB
		MaxBytes:    10e6, // 10MB
	})

	return reader
}

func main() {
	reader := newReader(Config{
		Brokers: []string{"localhost:9094"},
		GroupID: "consumer-group-id",
		Topics:  []string{"topic.a", "topic.b"},
	})

	ks := consumer.NewKafkaConsumer(
		context.Background(),
		reader,
		consumer.NewReactor(),
	)

	cv := counter.New()

	ks.RegisterHandler("topic.a", cv.HandleTopicA)
	ks.RegisterHandler("topic.b", cv.HandleTopicB)

	ks.StartConsuming()
}
