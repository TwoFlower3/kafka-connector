package alpha

import (
	"context"
	"fmt"
	"net"

	kafka "github.com/segmentio/kafka-go"

	"testing"
)

// Kafka struct
type TestKafka struct {
	client net.Conn

	topics map[string][]*kafka.Conn
}

func TestClientTopic(t *testing.T) {
	brokers := ""
	topics := []string{"test44", "test1", "test2", "test5", "test4"}
	klient := TestKafka{}

	if klient.topics == nil {
		klient.topics = make(map[string][]*kafka.Conn)
	}

	klient.client, _ = net.Dial("tcp", brokers)

	for _, topic := range topics {
		prtitions, _ := kafka.LookupPartitions(context.Background(), "tcp", brokers, topic)

		for i := range prtitions {
			fmt.Println(i)
			connKafka := kafka.NewConn(klient.client, topic, i)
			klient.topics[topic] = append(klient.topics[topic], connKafka)
		}
	}

	for _, topic := range topics {
		for _, partition := range klient.topics[topic] {
			partition.WriteMessages(kafka.Message{
				Key:   []byte("key"),
				Value: []byte("test"),
			})
		}
	}
}

func TestProduceMsgProtobuf(t *testing.T) {
	brokers := ""
	topics := []string{"test44"}
	klient := TestKafka{}

	if klient.topics == nil {
		klient.topics = make(map[string][]*kafka.Conn)
	}

	klient.client, _ = net.Dial("tcp", brokers)

	for _, topic := range topics {
		prtitions, _ := kafka.LookupPartitions(context.Background(), "tcp", brokers, topic)

		for i := range prtitions {
			fmt.Println(i)
			connKafka := kafka.NewConn(klient.client, topic, i)
			klient.topics[topic] = append(klient.topics[topic], connKafka)
		}
	}

	_, err := klient.topics["test44"][0].WriteMessages(kafka.Message{
		Key:   []byte("key"),
		Value: []byte("value"),
	})
	if err != nil {
		fmt.Println(err)
		t.Fatal(err)
	}
}

func TestConsumeMsgProtobuf(t *testing.T) {
	brokers := ""
	topics := []string{"test44"}
	klient := TestKafka{}

	if klient.topics == nil {
		klient.topics = make(map[string][]*kafka.Conn)
	}

	klient.client, _ = net.Dial("tcp", brokers)

	for _, topic := range topics {
		prtitions, _ := kafka.LookupPartitions(context.Background(), "tcp", brokers, topic)

		for i := range prtitions {
			fmt.Println(i)
			connKafka := kafka.NewConn(klient.client, topic, i)
			klient.topics[topic] = append(klient.topics[topic], connKafka)
		}
	}

	for {
		msg, err := klient.topics["test44"][0].ReadMessage(10e6)
		if err != nil {
			fmt.Println(err)
			break
		}

		fmt.Println(msg.Value)
	}
}
