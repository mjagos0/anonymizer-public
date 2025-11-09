package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

const (
	proxyRateLimit    = 60 * time.Second
	proxyTimeoutSlack = 3 * time.Second

	nginxLimit      = 1 << 20 // 1 MB
	batchQueueLimit = 1 << 8  // nxingLimit * 256 = 256 MB buffer
)

type Batch struct {
	buf        *bytes.Buffer
	lastOffset *kafka.TopicPartition
}

type Anonymizer struct {
	consumer *KafkaLogConsumer
	writer   *ClickhouseClient

	stagingBatch Batch
	batchQueue   chan Batch
	timer        *time.Timer
	retries      int
	dropped      int
}

func NewAnonymizer(consumer *KafkaLogConsumer, writer *ClickhouseClient) *Anonymizer {
	return &Anonymizer{
		consumer: consumer,
		writer:   writer,

		stagingBatch: Batch{buf: &bytes.Buffer{}},
		batchQueue:   make(chan Batch, batchQueueLimit),
		timer:        time.NewTimer(proxyRateLimit),
	}
}

func (a *Anonymizer) Run(ctx context.Context) {
	defer a.timer.Stop()
	log.Printf("Starting anonymizer, next batch upload scheduled after 60 seconds")

	// This design never sleeps or waits (apart from synchronous request to Clickhouse)
	for {
		select {
		case <-ctx.Done():
			return

		case <-a.timer.C:
			flushBatch, ok := a.nextFlushBatch()
			if !ok {
				continue
			}

			err := a.flushToClickhouse(&flushBatch)
			if err != nil {
				log.Print(err)
			}

			a.updateTimer(err == nil)
			a.commitOffset(&flushBatch)

		case logMsg := <-a.consumer.LogQueue:
			a.encodeRecord(logMsg)
		}
	}
}

// Send batch to Clickhouse. Handle server / network errors, panic on client error
// Print state of the buffer
func (a *Anonymizer) flushToClickhouse(flushBatch *Batch) error {
	sizeBytes := flushBatch.buf.Len()
	sizeMB := float64(sizeBytes) / (1024 * 1024)
	stagingMB := float64(a.stagingBatch.buf.Len()) / (1024 * 1024)
	queueMB := float64(len(a.batchQueue)) * (float64(nginxLimit) / (1024 * 1024))
	droppedMB := float64(a.dropped*nginxLimit) / (1024 * 1024)

	log.Printf(
		"Flushing batch (%.2f MB); buffer=[staging=%.2f MB, queue=%.2f/%d MB dropped=%.2f MB]",
		sizeMB,
		stagingMB,
		queueMB,
		batchQueueLimit,
		droppedMB,
	)

	statusCode, body, err := a.writer.executeInsert(
		fmt.Sprintf("INSERT INTO %s FORMAT RowBinary", clickhouseTable),
		"application/octet-stream",
		flushBatch.buf.Bytes(),
	)

	if err != nil {
		return fmt.Errorf("clickhouse request error %d: %s", statusCode, body)
	} else if statusCode >= 500 {
		return fmt.Errorf("clickhouse server error %d: %s", statusCode, body)
	} else if statusCode != 200 {
		panic(fmt.Errorf("clickhouse client error %d: %s", statusCode, body))
	}

	return nil
}

// Commit largest offset of a batch to Kafka
func (a *Anonymizer) commitOffset(flushBatch *Batch) {
	if flushBatch.lastOffset == nil {
		return
	}
	_, err := a.consumer.consumer.CommitMessage(&kafka.Message{
		TopicPartition: *flushBatch.lastOffset,
	})
	if err != nil {
		log.Printf("commit failed: %v", err)
	} else {
		log.Printf("Batch committed (topic: %s, partition: %d, offset: %d)",
			*flushBatch.lastOffset.Topic, flushBatch.lastOffset.Partition, flushBatch.lastOffset.Offset)
	}
}

// Update timer to proxyRateLimit + proxyTimeoutSlack if no error
// If error, exponential backoff starting at 3 seconds
func (a *Anonymizer) updateTimer(success bool) {
	if success {
		a.retries = 0
		a.timer.Reset(proxyRateLimit + proxyTimeoutSlack)
	} else {
		if a.retries == 0 {
			a.retries = 3
		} else {
			a.retries *= 2
			if a.retries > 30 {
				a.retries = 30
			}
		}
		a.timer.Reset(time.Duration(a.retries) * time.Second)
	}
}

// Consume batch from batchQueue, or consume stagingBatch if batchQueue empty
func (a *Anonymizer) nextFlushBatch() (Batch, bool) {
	select {
	case flushBatch := <-a.batchQueue:
		return flushBatch, true
	default:
	}

	// Fallback to staging batch
	if a.stagingBatch.buf.Len() > 0 {
		flushBatch := a.stagingBatch
		a.stagingBatch = Batch{buf: &bytes.Buffer{}}
		return flushBatch, true
	}

	return Batch{}, false
}

// Convert LogRecord to RowBinary Clickhouse format, append to stagingBatch
func (a *Anonymizer) encodeRecord(log LogRecord) {
	if a.stagingBatch.buf.Len()+log.Size >= nginxLimit {
		// Staging batch can't accommodate next message, not exact but safe
		select {
		case a.batchQueue <- a.stagingBatch:
		default:
			a.dropped++
			// TODO: Dump to external storage
		}
		a.stagingBatch = Batch{buf: &bytes.Buffer{}}
	}

	message := log.Message

	ts := uint32(message.TimestampEpochMilli() / 1000)
	resourceID := message.ResourceId()
	bytesSent := message.BytesSent()
	requestTimeMilli := message.RequestTimeMilli()
	responseStatus := uint16(message.ResponseStatus())
	cacheStatus, _ := message.CacheStatus()
	method, _ := message.Method()
	remoteAddr, _ := message.RemoteAddr()
	urlValue, _ := message.Url()

	var b [8]byte

	binary.LittleEndian.PutUint32(b[:4], ts)
	a.stagingBatch.buf.Write(b[:4])

	binary.LittleEndian.PutUint64(b[:], resourceID)
	a.stagingBatch.buf.Write(b[:])

	binary.LittleEndian.PutUint64(b[:], bytesSent)
	a.stagingBatch.buf.Write(b[:])

	binary.LittleEndian.PutUint64(b[:], requestTimeMilli)
	a.stagingBatch.buf.Write(b[:])

	binary.LittleEndian.PutUint16(b[:2], responseStatus)
	a.stagingBatch.buf.Write(b[:2])

	writeString(a.stagingBatch.buf, cacheStatus)
	writeString(a.stagingBatch.buf, method)
	writeString(a.stagingBatch.buf, anonymizeIP(remoteAddr))
	writeString(a.stagingBatch.buf, urlValue)

	a.stagingBatch.lastOffset = &log.Offset
}

func writeString(buf *bytes.Buffer, s string) {
	writeVarUInt(buf, uint64(len(s)))
	buf.WriteString(s)
}

func writeVarUInt(buf *bytes.Buffer, x uint64) {
	for x >= 0x80 {
		buf.WriteByte(byte(x) | 0x80)
		x >>= 7
	}
	buf.WriteByte(byte(x))
}

// Hide last octet of IPv4
func anonymizeIP(ip string) string {
	i := strings.LastIndexByte(ip, '.')
	if i > 0 {
		return ip[:i+1] + "X"
	}
	return ip
}
