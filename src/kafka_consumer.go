package main

import (
	"context"
	"fmt"
	"log"
	"time"

	capnp "capnproto.org/go/capnp/v3"
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

const (
	logQueueSize = 1 << 16
	dlqQueueSize = 1 << 8

	consumerReadTimeout = 10 * time.Second
)

type LogRecord struct {
	Message HttpLogRecord
	Offset  kafka.TopicPartition
	Size    int
}

type KafkaLogConsumer struct {
	consumer *kafka.Consumer
	LogQueue chan LogRecord
	dlq      chan *kafka.Message
}

// NewKafkaLogConsumer creates a Kafka consumer subscribed to the given topics.
// It initializes the logQueue and DLQ channels.
func NewKafkaLogConsumer(cfg *kafka.ConfigMap, topics []string) (*KafkaLogConsumer, error) {
	c, err := kafka.NewConsumer(cfg)
	if err != nil {
		return nil, fmt.Errorf("create consumer: %w", err)
	}

	if err := c.SubscribeTopics(topics, nil); err != nil {
		return nil, fmt.Errorf("subscribe to topics %v: %w", topics, err)
	}

	log.Printf("Kafka consumer created: %s", cfg)

	return &KafkaLogConsumer{
		consumer: c,
		LogQueue: make(chan LogRecord, logQueueSize),
		dlq:      make(chan *kafka.Message, dlqQueueSize),
	}, nil
}

// Run starts the consumer loop. It reads messages from Kafka, decodes them,
// and pushes valid log records into logQueue channel. Messages that cannot be decoded
// are sent to the DLQ. Messages that do not fit into the channel are thrown away.
func (c *KafkaLogConsumer) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			c.consumer.Close()
			return
		default:
			msg, err := c.consumer.ReadMessage(consumerReadTimeout)
			if err != nil {
				log.Printf("ReadMessage failed, retying in %s: %v", consumerReadTimeout, err)
				continue
			}

			logRecord, err := c.decodeMessage(msg)

			if err != nil {
				select {
				case c.dlq <- msg:
				default:
					log.Printf("DLQ full, dropping message from topic %s, partition %d, offset %d",
						*msg.TopicPartition.Topic, msg.TopicPartition.Partition, msg.TopicPartition.Offset)
				}
				continue
			}

			select {
			case c.LogQueue <- logRecord:
			default:
				// This should never happen
				log.Printf("logQueue full, dropping message from topic %s, partition %d, offset %d",
					*msg.TopicPartition.Topic, msg.TopicPartition.Partition, msg.TopicPartition.Offset)
			}
		}
	}
}

// Helper function to decode Kafka.message into DeserializedMessage
func (c *KafkaLogConsumer) decodeMessage(kafkaMessage *kafka.Message) (LogRecord, error) {
	capnpMsg, err := capnp.Unmarshal(kafkaMessage.Value)
	if err != nil {
		return LogRecord{}, err
	}

	httpLog, err := ReadRootHttpLogRecord(capnpMsg)
	if err != nil {
		return LogRecord{}, err
	}

	return LogRecord{
		Message: httpLog,
		Offset:  kafkaMessage.TopicPartition,
		Size:    len(kafkaMessage.Value),
	}, nil
}
