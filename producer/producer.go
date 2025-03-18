package main

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/segmentio/kafka-go"
)

func main() {
	topic := "hello-topic"
	partition := 0

	conn, err := kafka.DialLeader(context.Background(), "tcp", "localhost:9092", topic, partition)
	if err != nil {
		log.Fatal("failed to connect to Kafka leader:", err)
	}
	defer conn.Close()

	// Taking input from user
	fmt.Print("Enter message to send: ")
	reader := bufio.NewReader(os.Stdin)
	message, err := reader.ReadString('\n')
	if err != nil {
		log.Fatal("Error reading input:", err)
	}

	err = conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
	if err != nil {
		log.Fatal("failed to set write deadline:", err)
	}

	_, err = conn.WriteMessages(kafka.Message{Value: []byte(message)})
	if err != nil {
		log.Fatal("failed to write message:", err)
	}

	fmt.Println("Message sent successfully:", message)
}
