package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"

	_ "net/http/pprof"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

const (
	kafkaUrl   = "localhost:9092"
	kafkaGroup = "anonymizer"
	kafkaTopic = "http_log"

	clickhouseUrl   = "http://localhost:8124/"
	clickhouseTable = "http_log"
)

func main() {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM) // Allows goroutines to finish

	kafkaConsumer, err := NewKafkaLogConsumer(&kafka.ConfigMap{
		"bootstrap.servers":  kafkaUrl,
		"group.id":           kafkaGroup,
		"auto.offset.reset":  "earliest",
		"enable.auto.commit": false,
	}, []string{kafkaTopic})

	if err != nil {
		panic(fmt.Errorf("failed to create kafka consumer: %w", err))
	}

	clickhouse := NewClickhouseClient()
	anonymizer := NewAnonymizer(kafkaConsumer, clickhouse)

	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())

	wg.Add(1)
	go func() {
		defer wg.Done()
		kafkaConsumer.Run(ctx)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		anonymizer.Run(ctx)
	}()

	<-sigs
	fmt.Println("Shutting down...")
	cancel()

	wg.Wait()
	fmt.Println("Exited gracefully")
}
