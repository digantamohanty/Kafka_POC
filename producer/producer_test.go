package main

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"testing"
)

func TestProduceMessage(t *testing.T) {
	producer, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": "localhost:9092"})
	if err != nil {
		t.Fatalf("Failed to create Kafka producer: %v", err)
	}
	defer producer.Close()

	topic := "Notification"
	message := "Hello"

	errMsg := ProduceMessage(producer, topic, message)

	// Check if there's no error
	if errMsg != nil {
		t.Errorf("Expected no error, but got: %v", errMsg)
	}
}
