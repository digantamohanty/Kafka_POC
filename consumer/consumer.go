package main

import (
	"context"
	"fmt"
	"log"

	"github.com/segmentio/kafka-go"
)

func main() {
	topic := "hello-topic"
	partition := 0

	// Connect to Kafka
	conn, err := kafka.DialLeader(context.Background(), "tcp", "localhost:9092", topic, partition)
	if err != nil {
		log.Fatal("failed to connect to Kafka leader:", err)
	}
	defer conn.Close()

	fmt.Println("Listening for messages on topic:", topic)

	for {
		// Read message from Kafka
		message, err := conn.ReadMessage(1e6) // 1MB max message size
		if err != nil {
			log.Fatal("failed to read message:", err)
		}

		fmt.Printf("Received message: %s\n", string(message.Value))
	}
}
