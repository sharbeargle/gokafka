package main

import (
	"flag"
	"fmt"
	"github.com/Shopify/sarama"
	"os"
)

var (
	BROKERS = flag.String("brokers", os.Getenv("KAFKA_PEERS"), "The Kafka brokers to connect to, as a comma separated list")
	TOPIC   = "golangmessages"
)

func main() {
	consumer, err := sarama.NewConsumer(BROKERS, nil)
	if err != nil {
		fmt.Println("Failed to create Sarama consumer:", err)
		os.Exit(1)
	}

	subscribe(TOPIC, consumer)
}

func subscribe(topic string, consumer sarama.Consumer) {
	partitionList, err := consumer.Partitions(topic) //get all partitions on the given topic
	if err != nil {
		fmt.Println("Error retrieving partitionList ", err)
	}

	initialOffset := sarama.OffsetOldest //get offset for the oldest message on the topic

	for _, partition := range partitionList {
		pc, _ := consumer.ConsumePartition(topic, partition, initialOffset)

		go func(pc sarama.PartitionConsumer) {
			for message := range pc.Messages() {
				messageReceived(message)
			}
		}(pc)
	}
}

func messageReceived(message *sarama.ConsumerMessage) {
	fmt.Println(string(message.Value))
}
