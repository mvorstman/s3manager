package main

import (
	"context"
	"flag"
	"fmt"
	"log"
)

const version = "0.3"

func main() {
	action := flag.String("action", "list", "Action to perform: list")
	endpoint := flag.String("endpoint", "", "S3 endpoint, for example https://s3.example.local")
	region := flag.String("region", "us-east-1", "AWS region")
	accessKey := flag.String("access-key", "", "S3 access key")
	secretKey := flag.String("secret-key", "", "S3 secret key")
	bucket := flag.String("bucket", "", "Bucket name")
	prefix := flag.String("prefix", "", "Optional prefix filter")
	maxKeys := flag.Int("max-keys", 1000, "Max objects per request (pagination size)")

	flag.Parse()

	fmt.Println("S3Manager v" + version)

	if *endpoint == "" || *accessKey == "" || *secretKey == "" || *bucket == "" {
		log.Fatal("endpoint, access-key, secret-key, and bucket are required")
	}

	ctx := context.Background()

	client, err := newS3Client(ctx, *endpoint, *region, *accessKey, *secretKey)
	if err != nil {
		log.Fatalf("failed to create S3 client: %v", err)
	}

	switch *action {
	case "list":
		listAllObjects(ctx, client, *bucket, *prefix, int32(*maxKeys))
	default:
		log.Fatalf("unknown action: %s", *action)
	}
}