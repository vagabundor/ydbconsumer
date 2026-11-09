package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicoptions"
)

func main() {
	// Flags
	var (
		endpoint     = flag.String("endpoint", "", "YDB endpoint")
		database     = flag.String("database", "", "YDB database path")
		user         = flag.String("user", "", "YDB user login")
		password     = flag.String("password", "", "YDB user password")
		caFile       = flag.String("ca_file", "", "Path to CA certificate")
		topicName    = flag.String("topic", "", "YDB topic to read from")
		consumerName = flag.String("consumer", "test-consumer", "Consumer name")
		batchSize    = flag.Int("batch_size", 100, "Max messages per batch")
		timeout      = flag.Duration("read_timeout", 5*time.Second, "Timeout for reading a batch")
		startOffset  = flag.Int64("start_offset", -1, "Read messages starting from this offset "+
			"(applied to every partition). Negative value means read from the consumer's committed offset")
	)
	flag.Parse()

	// Context with cancellation on SIGINT/SIGTERM
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Connect to YDB
	db, err := ydb.Open(ctx,
		fmt.Sprintf("grpcs://%s?database=%s", *endpoint, *database),
		ydb.WithStaticCredentials(*user, *password),
		ydb.WithCertificatesFromFile(*caFile),
	)
	if err != nil {
		log.Fatalf("failed to connect to YDB: %v", err)
	}
	defer db.Close(ctx)

	// Reader setup
	readerOpts := []topicoptions.ReaderOption{
		topicoptions.WithReaderBatchMaxCount(*batchSize),
	}
	if *startOffset >= 0 {
		readerOpts = append(readerOpts, topicoptions.WithReaderGetPartitionStartOffset(
			func(ctx context.Context, req topicoptions.GetPartitionStartOffsetRequest) (topicoptions.GetPartitionStartOffsetResponse, error) {
				log.Printf("Starting partition %d of topic %q from offset %d", req.PartitionID, req.Topic, *startOffset)
				var res topicoptions.GetPartitionStartOffsetResponse
				res.StartFrom(*startOffset)
				return res, nil
			},
		))
	}

	reader, err := db.Topic().StartReader(
		*consumerName,
		topicoptions.ReadTopic(*topicName),
		readerOpts...,
	)
	if err != nil {
		log.Fatalf("failed to start topic reader: %v", err)
	}
	defer reader.Close(ctx)

	if *startOffset >= 0 {
		log.Printf("Reading from offset: %d (per partition)", *startOffset)
	} else {
		log.Printf("Reading from the consumer's committed offset")
	}
	log.Printf("Starting to read messages from topic: %s", *topicName)

	for {
		select {
		case <-ctx.Done():
			log.Println("Shutdown signal received, exiting...")
			return

		default:
			readCtx, cancel := context.WithTimeout(ctx, *timeout)
			batch, err := reader.ReadMessagesBatch(readCtx)
			cancel()

			if err != nil {
				if readCtx.Err() == context.DeadlineExceeded {
					continue
				}
				log.Printf("error reading batch: %v", err)
				time.Sleep(time.Second)
				continue
			}

			for _, msg := range batch.Messages {
				buf := new(bytes.Buffer)
				_, err := io.Copy(buf, msg)
				if err != nil {
					log.Printf("failed to read message body: %v", err)
					continue
				}
				fmt.Println("===========================")
				fmt.Printf("Message WrittenAt: %s\n", msg.WrittenAt)
				fmt.Printf("Message CreatedAt: %s\n", msg.CreatedAt)
				fmt.Printf("Message WriteSessionMetadata: %s\n", msg.WriteSessionMetadata)
				fmt.Printf("Message Offset: %d\n", msg.Offset)
				fmt.Printf("Message ProducerID: %s\n", msg.ProducerID)
				fmt.Printf("Message SeqNo: %d\n", msg.SeqNo)
				fmt.Printf("Message PartitionID: %d\n", msg.PartitionID())

				if len(msg.Metadata) > 0 {
					fmt.Println("Metadata:")
					for k, v := range msg.Metadata {
						fmt.Printf("  %s = %s\n", k, string(v))
					}
				}

				fmt.Printf("Message body: %s\n", buf.String())

				if err := reader.Commit(ctx, msg); err != nil {
					log.Printf("commit failed: %v", err)
				}
			}
		}
	}
}
