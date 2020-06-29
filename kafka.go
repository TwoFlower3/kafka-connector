package alpha

import (
	"context"
	"fmt"
	"net"
	"strings"
	"time"

	kafka "github.com/segmentio/kafka-go"
)

// Kafka struct
type Kafka struct {
	client  net.Conn
	ctx     context.Context
	broker  string
	groupID string

	LeadTopic     string
	LeadPartition int

	Topics map[string][]*kafka.Conn
}

// CreateClient connection to kafka
func (klient *Kafka) CreateClient(address, groupID string) error {
	var err error
	klient.client, err = net.Dial("tcp", address)
	if err != nil {
		return fmt.Errorf("kafka connection error: %+v", err)
	}

	if klient.Topics == nil {
		klient.Topics = make(map[string][]*kafka.Conn)
	}

	klient.ctx = context.Background()
	klient.broker = address
	klient.groupID = groupID

	return nil
}

// SetLeadTopic dummy
func (klient *Kafka) SetLeadTopic(topic string, partition int) {
	klient.LeadTopic = topic
	klient.LeadPartition = partition
}

// CreateTopicConnect dummy
func (klient *Kafka) CreateTopicConnect(topic string, partition int) {
	connTopic := kafka.NewConn(klient.client, topic, partition)
	klient.Topics[topic] = append(klient.Topics[topic], connTopic)
}

// CreateTopicsConnect dummy
func (klient *Kafka) CreateTopicsConnect(topics ...string) {
	for _, topic := range topics {
		partitions, err := kafka.LookupPartitions(klient.ctx, "tcp", klient.broker, topic)
		if err != nil {
			fmt.Printf("LookupPartitions error: %+v", err) // logger
			continue
		}

		for i := range partitions {
			connTopic := kafka.NewConn(klient.client, topic, i)
			klient.Topics[topic] = append(klient.Topics[topic], connTopic)
		}
	}
}

// ProduceMsg produce msg to topic
func (klient *Kafka) ProduceMsg(topic, key, message string) error {
	for _, partition := range klient.Topics[topic] {
		_, err := partition.WriteMessages(kafka.Message{
			Key:   []byte(key),
			Value: []byte(message),
		})

		if err != nil {
			if strings.Contains(err.Error(), "dummy") { // TODO: what error equal to connection refused
				return fmt.Errorf("error: %+v", err)
			}
			return fmt.Errorf("produce error: %+v", err)
		}
	}

	return nil
}

// CheckOffset ...
func (klient *Kafka) CheckOffset(reader *kafka.Reader) int64 {
	offset := reader.Stats()

	return offset.Offset
}

// CreateReader ...
func (klient *Kafka) CreateReader(topic string, partition int) (*kafka.Reader, error) {

	var brokersAddr []string

	fmt.Println(topic, klient.Topics[topic])

	topicPartition := klient.Topics[topic][partition]

	brokers, err := topicPartition.Brokers()
	if err != nil {
		return nil, fmt.Errorf("get brokers error: %+v", err)
	}

	for _, broker := range brokers {
		if broker.Host == "" {
			continue
		}

		brokerAddr := broker.Host
		if broker.Port != 0 {
			brokerAddr = fmt.Sprintf("%s:%d", brokerAddr, broker.Port)
		}

		brokersAddr = append(brokersAddr, brokerAddr)
	}

	if len(brokersAddr) == 0 {
		return nil, fmt.Errorf("partition of topic have empty brokers")
	}

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        brokersAddr,
		Topic:          topic,
		GroupID:        klient.groupID,
		MinBytes:       10e3,        // 10KB
		MaxBytes:       10e6,        // 10MB
		CommitInterval: time.Second, // flushes commits to Kafka every second
	})

	return reader, nil
}

// ConsumeMsg ...
func (klient *Kafka) ConsumeMsg(reader *kafka.Reader, job func(msg kafka.Message) error) error {

	msg, err := reader.FetchMessage(klient.ctx)
	if err != nil {
		if strings.Contains(err.Error(), "Not Leader For Partition") {
			return nil
		}
		return fmt.Errorf("fetch message error: %+v", err)
	}

	err = job(msg)
	if err != nil {
		fmt.Printf("job can't be done offset: %d, time: %v", msg.Offset, msg.Time) // logger
	}

	commit := func() error {
		err = reader.CommitMessages(klient.ctx, msg)
		if err != nil {
			return fmt.Errorf("commit offset error: %+v", err)
		}
		return nil
	}

	err = commit()
	if err != nil {
		return err
	}

	return nil
}

// ReconnectTopic dummy
func (klient *Kafka) ReconnectTopic(topic string) {

	klient.Topics[topic] = []*kafka.Conn{}

	klient.CreateTopicsConnect(topic)
}
