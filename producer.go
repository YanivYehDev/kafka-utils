package kafkautils


import (
	"context"
	"time"

	"github.com/segmentio/kafka-go"
)

// KafkaProducer wraps Kafka writing logic
type KafkaProducer struct {
	Writer *kafka.Writer
}

// NewKafkaProducer initializes a new Kafka producer
func NewKafkaProducer(brokers []string, topic string) *KafkaProducer {
	return &KafkaProducer{
		Writer: &kafka.Writer{
			Addr:         kafka.TCP(brokers...),
			Topic:        topic,
			Balancer:     &kafka.LeastBytes{},
			BatchTimeout: 1 * time.Second,
		},
	}
}

// SendMessage sends a message to Kafka
func (p *KafkaProducer) SendMessage(message string) error {
	return p.Writer.WriteMessages(context.Background(), kafka.Message{
		Value: []byte(message),
	})
}

// Close shuts down the producer
func (p *KafkaProducer) Close() error {
	return p.Writer.Close()
}