package main

import (
	"testing"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/go-redis/redis"
)

func TestConsumer(t *testing.T) {
	// Setup Kafka consumer
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
		"group.id":          "test-group",
		"auto.offset.reset": "earliest",
	})
	if err != nil {
		t.Fatalf("Failed to create Kafka consumer: %v", err)
	}
	defer consumer.Close()

	// Subscribe to test topic
	topic := "Notification"
	err = consumer.SubscribeTopics([]string{topic}, nil)
	if err != nil {
		t.Fatalf("Failed to subscribe to topic: %v", err)
	}

	// Setup Redis client
	client := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	})
	defer client.Close()

	// Consume message from Kafka (wait for a short time)
	timeout := time.After(5 * time.Second)
	done := make(chan bool)

	go func() {
		for {
			select {
			case <-timeout:
				done <- false
				return
			default:
				if msg, err := consumer.ReadMessage(100 * time.Millisecond); err == nil {
					client.Set("notification", string(msg.Value), 0)
					val, _ := client.Get("notification").Result()
					done <- (val == string(msg.Value))
					return
				}
			}
		}
	}()

	if success := <-done; !success {
		t.Fatal("Test failed: No message received or Redis mismatch")
	} else {
		t.Log("Test passed: Message consumed and stored in Redis")
	}

}
