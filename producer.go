package main

import (
	"fmt"
	"github.com/Shopify/sarama"
	"strings"
	"flag"
	"os"
	"bufio"
)

var (
	BROKERS = flag.String("brokers", os.Getenv("KAFKA_PEERS"), "The Kafka brokers to connect to, as a comma separated list (ie -brokers=kafkasrv1,kafkasrv2,kafkasrv3)")
	TOPIC   = flag.String("topic", "golangmessages", "The Kafka brokers to connect to, as a comma separated list")
)

func main() {
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
		fmt.Println("Enter message: ")
		message, _ := reader.ReadString('\n')
		message = strings.TrimSpace(message)

		if message == "" {
			fmt.Println("Exiting")
			os.Exit(0)
		}

		msg := prepareMessage(*TOPIC, message)
		partition, offset, err := producer.SendMessage(msg)
		fmt.Printf("Message was saved to partion: %d.\nMessage offset is: %d.\n %s error occured.\n", partition, offset, err.Error())
	}
}

func newProducer(brokerList []string) (sarama.SyncProducer, error) {
	config := sarama.NewConfig()
	//config.Producer.Partitioner = sarama.NewRandomPartitioner
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
