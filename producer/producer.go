package main

import (
	"bufio"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"log"
	"os"
)

func main() {
	// Creating a kafka producer
	producer, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": "localhost:9092"})
	if err != nil {
		log.Fatal(err)
	}
	defer producer.Close()

	topic := "Notification"
	for {
		fmt.Print("Enter message to send: ")
		reader := bufio.NewReader(os.Stdin)
		messageToSend, err := reader.ReadString('\n')
		if err != nil {
			log.Println("Error reading input:", err)
			continue
		}

		// Send message
		err = producer.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Value:          []byte(messageToSend),
		}, nil)

		fmt.Println("Message sent successfully:", messageToSend)
	}
}
