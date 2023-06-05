package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

const (
	kafkaBroker = "localhost:9094"
)

func main() {
	go publishMessages()

	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, os.Interrupt)
	<-sigchan
}

func toPtr(s string) *string {
	return &s
}

func publishMessages() {
	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": kafkaBroker})
	if err != nil {
		log.Fatal(err)
	}

	defer p.Close()

	for i := 0; i < 10; i++ {
		message := fmt.Sprintf("Message %d", i)
		err := p.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: toPtr("topic.a"), Partition: kafka.PartitionAny},
			Value:          []byte(message),
		}, nil)

		if err != nil {
			log.Printf("Failed to produce message: %v\n", err)
		}

		fmt.Println("Published from topic a message: ", message)

		err = p.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: toPtr("topic.b"), Partition: kafka.PartitionAny},
			Value:          []byte(message),
		}, nil)

		if err != nil {
			log.Printf("Failed to produce message: %v\n", err)
		}

		fmt.Println("Published from topic b message: ", message)
	}

	go deliveryReport(p.Events())

	p.Flush(15 * 1000) // wait for delivery report or 15 seconds
}

func deliveryReport(e chan kafka.Event) {
	for ev := range e {
		switch ev.(type) {
		case *kafka.Message:
			m := ev.(*kafka.Message)
			if m.TopicPartition.Error != nil {
				fmt.Printf("Delivery failed: %v\n", m.TopicPartition.Error)
			} else {
				fmt.Printf("Delivered message to topic %s [%d] at offset %v\n",
					*m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset)
			}
		}
	}
}
