package main

import (
	"context"
	"flag"
	"fmt"
	"log"

	s3pkg "s3manager/internal/s3"
)

const version = "0.14"

func main() {
	action := flag.String("action", "list", "Action to perform: list | upload | upload-folder | download | download-prefix | head | delete")
	endpoint := flag.String("endpoint", "", "S3 endpoint")
	region := flag.String("region", "us-east-1", "AWS region")
	accessKey := flag.String("access-key", "", "S3 access key")
	secretKey := flag.String("secret-key", "", "S3 secret key")
	bucket := flag.String("bucket", "", "Bucket name")
	prefix := flag.String("prefix", "", "Optional prefix filter")
	maxKeys := flag.Int("max-keys", 1000, "Max objects per request")

	workers := flag.Int("workers", 4, "Number of workers")
	verbose := flag.Bool("verbose", false, "Verbose output")

	maxAttempts := flag.Int("max-attempts", 5, "Retry attempts")
	retryMaxBackoffMs := flag.Int("retry-max-backoff-ms", 5000, "Retry backoff")

	objectKey := flag.String("key", "", "Object key")
	filePath := flag.String("file", "", "File to upload")
	folderPath := flag.String("folder", "", "Folder to upload")
	keyPrefix := flag.String("key-prefix", "", "Key prefix")
	outputPath := flag.String("out", "", "Output file")
	outputDir := flag.String("out-dir", "", "Output dir")
	dryRun := flag.Bool("dry-run", true, "Dry run delete")

	flag.Parse()

	// temporary: mark unused flags as used during refactor
	_ = workers
	_ = verbose
	_ = objectKey
	_ = filePath
	_ = folderPath
	_ = keyPrefix
	_ = outputPath
	_ = outputDir
	_ = dryRun

	fmt.Println("S3Manager v" + version)

	ctx := context.Background()

	client, err := s3pkg.NewClient(ctx, s3pkg.ClientConfig{
		Endpoint:          *endpoint,
		Region:            *region,
		AccessKey:         *accessKey,
		SecretKey:         *secretKey,
		UsePathStyle:      true,
		MaxAttempts:       *maxAttempts,
		RetryMaxBackoffMs: *retryMaxBackoffMs,
	})
	if err != nil {
		log.Fatalf("failed to create client: %v", err)
	}

	switch *action {
	case "list":
		_, err := s3pkg.ListObjects(ctx, client, *bucket, *prefix, int32(*maxKeys))
		if err != nil {
			log.Fatalf("list failed: %v", err)
		}

	default:
		log.Fatalf("unknown action: %s", *action)
	}
}