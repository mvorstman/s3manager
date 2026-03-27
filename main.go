package main

import (
	"context"
	"flag"
	"fmt"
	"log"
)

const version = "0.9"

func main() {
	action := flag.String("action", "list", "Action to perform: list | upload | upload-folder | download | delete")
	endpoint := flag.String("endpoint", "", "S3 endpoint, for example https://s3.example.local")
	region := flag.String("region", "us-east-1", "AWS region")
	accessKey := flag.String("access-key", "", "S3 access key")
	secretKey := flag.String("secret-key", "", "S3 secret key")
	bucket := flag.String("bucket", "", "Bucket name")
	prefix := flag.String("prefix", "", "Optional prefix filter")
	maxKeys := flag.Int("max-keys", 1000, "Max objects per request (pagination size)")

	// Shared object flags
	objectKey := flag.String("key", "", "Object key in S3")

	// Upload-specific flags
	filePath := flag.String("file", "", "Local file to upload")
	folderPath := flag.String("folder", "", "Local folder to upload recursively")
	keyPrefix := flag.String("key-prefix", "", "Optional S3 key prefix for folder uploads")
	workers := flag.Int("workers", 4, "Number of parallel upload workers for folder upload")

	// Download-specific flag
	outputPath := flag.String("out", "", "Local output file path for download")

	// Delete-specific flag
	dryRun := flag.Bool("dry-run", true, "If true, only show what would be deleted")

	flag.Parse()

	fmt.Println("S3Manager v" + version)

	if *endpoint == "" || *accessKey == "" || *secretKey == "" || *bucket == "" {
		log.Fatal("endpoint, access-key, secret-key, and bucket are required")
	}

	if *workers < 1 {
		log.Fatal("--workers must be at least 1")
	}

	ctx := context.Background()

	client, err := newS3Client(ctx, *endpoint, *region, *accessKey, *secretKey)
	if err != nil {
		log.Fatalf("failed to create S3 client: %v", err)
	}

	switch *action {
	case "list":
		listAllObjects(ctx, client, *bucket, *prefix, int32(*maxKeys))

	case "upload":
		if *filePath == "" || *objectKey == "" {
			log.Fatal("for upload, both --file and --key are required")
		}
		uploadFile(ctx, client, *bucket, *filePath, *objectKey)

	case "upload-folder":
		if *folderPath == "" {
			log.Fatal("for upload-folder, --folder is required")
		}
		uploadFolder(ctx, client, *bucket, *folderPath, *keyPrefix, *workers)

	case "download":
		if *objectKey == "" || *outputPath == "" {
			log.Fatal("for download, both --key and --out are required")
		}
		downloadFile(ctx, client, *bucket, *objectKey, *outputPath)

	case "delete":
		deleteObjectsByPrefix(ctx, client, *bucket, *prefix, int32(*maxKeys), *dryRun)

	default:
		log.Fatalf("unknown action: %s", *action)
	}
}