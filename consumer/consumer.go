package main

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"log"
)

func main() {
	topic := "Notification"

	// Creating a kafka consumer
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
		"group.id":          "notification-group",
		"auto.offset.reset": "earliest",
	})
	defer consumer.Close()

	fmt.Println("Listening for messages on topic:", topic)

	err = consumer.SubscribeTopics([]string{topic}, nil)
	if err != nil {
		fmt.Printf("Failed to subscribe to topic: %s\n", err)
		return
	}

	for {
		// Read message from Kafka
		message, consumerErr := consumer.ReadMessage(-1)
		if consumerErr != nil {
			log.Fatal("failed to read message:", consumerErr)
		} else {
			fmt.Printf("Received message: %s\n", string(message.Value))
		}
	}
}
