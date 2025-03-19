package main

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/go-redis/redis"
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

	// Connecting to redis
	client := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	})
	_, redisError := client.Ping().Result()
	if redisError != nil {
		panic(redisError)
	} else {
		fmt.Println("Conected to redis")
	}

	fmt.Println("Listening for messages on topic:", topic)

	SubscribeErr = consumer.SubscribeTopics([]string{topic}, nil)
	if SubscribeErr != nil {
		fmt.Printf("Failed to subscribe to topic: %s\n", SubscribeErr)
		return
	}

	for {
		// Read message from Kafka
		message, consumerErr := consumer.ReadMessage(-1)
		if consumerErr != nil {
			log.Fatal("failed to read message:", consumerErr)
		} else {
			fmt.Printf("Received message at consumer: %s\n", string(message.Value))
			errRedis := client.Set("notification", string(message.Value), 0).Err()
			if errRedis != nil {
				log.Println("Redis error:", errRedis)
			}
		}

		// Retrieve and print the message from Redis
		val, err := client.Get("notification").Result()
		if err != nil {
			log.Println("Error retrieving from Redis:", err)
		} else {
			fmt.Println("Data from Redis:", val)
		}
	}
}
