package main

import (
	"fmt"
	"github.com/Shopify/sarama"
	"flag"
	"strings"
	"log"
	"bufio"
	"os"
)

var (
	BROKERS = flag.String("brokers", os.Getenv("KAFKA_PEERS"), "The Kafka brokers to connect to, as a comma separated list")
	VERBOSE = flag.Bool("verbose", false, "Turn on Sarama logging")
)

func main() {
	if *verbose {
		sarama.Logger = log.New(os.Stdout, "[sarama] ", log.LstdFlags)
	}

	if *brokers == "" {
		flag.PrintDefaults()
		os.Exit(1)
	}

	brokerList := strings.Split(*brokers, ",")
	log.Printf("Kafka brokers: %s", strings.Join(brokerList, ", "))

	server := &Server{
		MessageProducer: newMessageProducer(brokerList),
	}

	defer func() {
		if err := server.Close(); err != nil {
			log.Println("Failed to close server", err)
		}
	}()

	server.Run()
}

type Server struct {
	MessageProducer sarama.AsyncProducer
}

func (s *Server) Close() error {
	if err := s.MessageProducer.Close(); err != nil {
		log.Println("Failed to shut down data AsyncProducer cleanly", err)
	}

	return nil
}

func (s *Server) Run() {
	reader := bufio.NewReader(os.Stdin)
	for {
		fmt.Println("")
		fmt.Print("Enter Key: ")
		key, _ := reader.ReadString('\n')
		fmt.Print("Enter Value: ")
		value, _ := reader.ReadString('\n')

		if key == "" {
			return
		} else {
			s.SendMessage(key, value)
		}
	}
}

func (s *Server) SendMessage(key string, message string) {
	s.MessageProducer.Input() <- &sarama.ProducerMessage {
		Topic: "golangmessages",
		Key: key,
		Value: message
	}
}

func newMessageProducer(brokerList []string) sarama.AsyncProducer {
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForLocal
	config.Producer.Compression = sarama.CompressionSnappy
	config.Producer.Flush.Frequency = 500 * time.Millisecond
	config.Net.TLS.Enable = false

	producer, err := sarama.NewAsyncProducer(brokerList, config)
	if err != nil {
		log.Fatalln("Failed to start Sarama producer:", err)
	}

	go func() {
		for err := range producer.Errors() {
			log.Println("Failed to write access log entry:", err)
		}
	}()

	return producer
}
