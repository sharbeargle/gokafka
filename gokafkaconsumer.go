package main

import (
	"flag"
	"fmt"
	"github.com/Shopify/sarama"
	"os"
	"strings"
	"bufio"
)

var (
	BROKERS = flag.String("brokers", os.Getenv("KAFKA_PEERS"), "The Kafka brokers to connect to, as a comma separated list")
	TOPIC   = "golangmessages"
)

// Simple demo of creating a Kafka consumer
// This will get all messages from all partitions of a topic
func main() {
	brokerList := strings.Split(*BROKERS, ",")

	consumer, err := sarama.NewConsumer(brokerList, nil)
	if err != nil {
		fmt.Println("Failed to create Sarama consumer:", err)
		os.Exit(1)
	}

	fmt.Println("--Hit enter to quit--")

	subscribe(TOPIC, consumer)

	bufio.NewReader(os.Stdin).ReadString('\n')
}

func subscribe(topic string, consumer sarama.Consumer) {
	// Get all partitions on the given topic
	partitionList, err := consumer.Partitions(topic) 
	if err != nil {
		fmt.Println("Error retrieving partitionList ", err)
	}

	// Get offset for the oldest message on the topic
	initialOffset := sarama.OffsetOldest 

	for _, partition := range partitionList {
		pc, _ := consumer.ConsumePartition(topic, partition, initialOffset)

		go func(pc sarama.PartitionConsumer) {
			for message := range pc.Messages() {
				messageReceived(message)
			}
		}(pc)
	}
}

// Do something with the new message
func messageReceived(message *sarama.ConsumerMessage) {
	fmt.Println(string(message.Value))
}
