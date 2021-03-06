package main

import (
	"bufio"
	"flag"
	"fmt"
	"github.com/Shopify/sarama"
	"os"
	"strings"
)

var (
	BROKERS = flag.String("brokers", os.Getenv("KAFKA_PEERS"), "The Kafka brokers to connect to, as a comma separated list (ie -brokers=kafkasrv1:9092,kafkasrv2:9092,kafkasrv3.13:9092)")
	TOPIC   = flag.String("topic", "golangmessages", "The Kafka brokers to connect to, as a comma separated list")
)

func main() {
	flag.Parse()
	
	if *BROKERS == "" {
		fmt.Println("No brokers available")
		os.Exit(1)
	}

	brokerList := strings.Split(*BROKERS, ",")

	producer, err := newProducer(brokerList)
	if err != nil {
		fmt.Println("Failed to create Sarama producer:", err)
		os.Exit(1)
	}

	reader := bufio.NewReader(os.Stdin)

	for {
		fmt.Printf("Enter message: ")
		message, _ := reader.ReadString('\n')
		message = strings.TrimSpace(message)

		if message == "" {
			fmt.Println("Exiting")
			os.Exit(0)
		}

		msg := prepareMessage(*TOPIC, message)
		partition, offset, _ := producer.SendMessage(msg)
		fmt.Printf("Message was saved to partion: %d.\nMessage offset is: %d.\n", partition, offset)
	}
}

func newProducer(brokerList []string) (sarama.SyncProducer, error) {
	config := sarama.NewConfig()
	config.Producer.Retry.Max = 10
	config.Producer.Return.Successes = true
	config.Net.TLS.Enable = false
	config.Producer.RequiredAcks = sarama.WaitForAll
	producer, err := sarama.NewSyncProducer(brokerList, config)

	return producer, err
}

func prepareMessage(topic, message string) *sarama.ProducerMessage {
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(message),
	}

	return msg
}
