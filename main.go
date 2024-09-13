package main

import (
	"fmt"
	"log"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type orderProducer struct {
	producer     *kafka.Producer
	topic        string
	deliveryChan chan kafka.Event
}

func newOrderProducer(p *kafka.Producer, topic string) *orderProducer {
	return &orderProducer{
		producer:     p,
		topic:        topic,
		deliveryChan: make(chan kafka.Event, 1000),
	}
}

func (op *orderProducer) placeOrder(orderType string, size int) error {
	var (
		format  = fmt.Sprintf("%s - %d ", orderType, size)
		payload = []byte(format)
	)

	err := op.producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &op.topic,
			Partition: kafka.PartitionAny,
		},
		Value: payload,
	}, op.deliveryChan)
	if err != nil {
		return err
	}
	<-op.deliveryChan
	fmt.Printf("order placed in the queue: %s \n", format)
	return nil
}

func main() {
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
		"client.id":         "ID",
		"acks":              "all",
	})
	if err != nil {
		fmt.Printf("Failed to create producer: %s\n", err)
	}

	fmt.Printf("%+v \n", p)

	op := newOrderProducer(p, "topic")
	for i := 1; i <= 1000; i++ {
		if err := op.placeOrder("market", i); err != nil {
			log.Fatal(err)
		}
		time.Sleep(time.Second * 3)
	}

}
