package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/Shopify/sarama"
	"log"
	"os"
	"strings"
	"time"
)

var (
	BROKERS = flag.String("brokers", os.Getenv("KAFKA_PEERS"), "The Kafka brokers to connect to, as a comma separated list")
	VERBOSE = flag.Bool("verbose", false, "Turn on Sarama logging")
)

func main() {
	if *VERBOSE {
		sarama.Logger = log.New(os.Stdout, "[sarama] ", log.LstdFlags)
	}

	if *BROKERS == "" {
		flag.PrintDefaults()
		os.Exit(1)
	}

	brokerList := strings.Split(*BROKERS, ",")
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

// Struct holds message, implements sarama.Encoder
type Message struct {
	Name          string `json:"name"`
	MessageString string `json:"message"`
	encoded       []byte
	err           error
}

func (m *Message) EnsureEncoded() {
	if m.encoded == nil && m.err == nil {
		m.encoded, m.err = json.Marshal(m)
	}
}

func (m *Message) Encode() ([]byte, error) {
	m.EnsureEncoded()
	return m.encoded, m.err
}

func (m *Message) Length() int {
	m.EnsureEncoded()
	return len(m.encoded)
}

type Server struct {
	MessageProducer sarama.SyncProducer
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
		fmt.Print("Enter Name: ")
		key, _ := reader.ReadString('\n')
		fmt.Print("Enter Message: ")
		value, _ := reader.ReadString('\n')

		fmt.Println(key, value)

		message := &Message{
			Name:          key,
			MessageString: value,
		}

		s.SendMessage(message)
	}
}

func (s *Server) SendMessage(message *Message) {
	partition, offset, err := s.MessageProducer.SendMessage(&sarama.ProducerMessage{
		Topic: "golangmessages",
		Value: message,
	})

	if err != nil {
		fmt.Fprintf(w, "Failed to store your data:, %s", err)
	} else {
		// The tuple (topic, partition, offset) can be used as a unique identifier
		// for a message in a Kafka cluster.
		fmt.Fprintf(w, "Your data is stored with unique identifier important/%d/%d", partition, offset)
	}
}

func newMessageProducer(brokerList []string) sarama.SyncProducer {
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 10
	config.Producer.Return.Successes = true
	config.Net.TLS.Enable = false

	fmt.Println(brokerlist)
	producer, err := sarama.NewSyncProducer(brokerList, config)
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
