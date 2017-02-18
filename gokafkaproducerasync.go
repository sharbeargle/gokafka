package main

import (
	"fmt"
	"github.com/Shopify/sarama"
	"flag"
	"strings"
	"log"
	"bufio"
	"os"
	"time"
	"encoding/json"
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
	Name			string `json:"name"`
	MessageString	string`json:"message"`
	encoded			[]byte
	err				error
}

func (m *Message) EnsureEncoded() () {
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
		fmt.Print("Enter Name: ")
		key, _ := reader.ReadString('\n')
		fmt.Print("Enter Message: ")
		value, _ := reader.ReadString('\n')

		message := &Message{
			Name: key,
			MessageString: value,
		}

		if key == "\n" {
			return
		} else {
			s.SendMessage(key, message)
		}
	}
}

func (s *Server) SendMessage(key string, message *Message) {
	s.MessageProducer.Input() <- &sarama.ProducerMessage {
		Topic: "golangmessages",
		Key: sarama.StringEncoder(key),
		Value: message,
	}
}

func newMessageProducer(brokerList []string) sarama.AsyncProducer {
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForLocal
	config.Producer.Compression = sarama.CompressionSnappy
	config.Producer.Flush.Frequency = 500 * time.Millisecond
	config.Net.TLS.Enable = false

	fmt.Println(brokerlist)
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
