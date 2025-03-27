package kafkautils

import (
	"context"
	"fmt"
	"log"

	"github.com/segmentio/kafka-go"
)

// KafkaConsumer wraps Kafka reading logic
type KafkaConsumer struct {
	Reader *kafka.Reader
}

// NewKafkaConsumer initializes a Kafka consumer
func NewKafkaConsumer(brokers []string, topic, groupID string) *KafkaConsumer {
	return &KafkaConsumer{
		Reader: kafka.NewReader(kafka.ReaderConfig{
			Brokers: brokers,
			Topic:   topic,
			GroupID: groupID,
		}),
	}
}

// ReadMessages listens for Kafka messages
func (c *KafkaConsumer) ReadMessages() {
	fmt.Println("⏳ Waiting for messages...")
	for {
		msg, err := c.Reader.ReadMessage(context.Background())
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("📩 Received: %s\n", string(msg.Value))
	}
}

// Close shuts down the consumer
func (c *KafkaConsumer) Close() error {
	return c.Reader.Close()
}
