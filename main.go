package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"

	enginepkg "s3manager/internal/engine"
	s3pkg "s3manager/internal/s3"
)

const version = "0.2.0"

func flagOrEnv(flagValue string, envKeys ...string) string {
	if flagValue != "" {
		return flagValue
	}
	for _, key := range envKeys {
		if value := os.Getenv(key); value != "" {
			return value
		}
	}
	return ""
}

func main() {
	action := flag.String("action", "list", "Action to perform: list | upload | upload-folder | download | download-prefix | head | delete")
	endpoint := flag.String("endpoint", "", "S3 endpoint, for example https://s3.example.local")
	region := flag.String("region", "us-east-1", "AWS region")
	accessKey := flag.String("access-key", "", "S3 access key")
	secretKey := flag.String("secret-key", "", "S3 secret key")
	bucket := flag.String("bucket", "", "Bucket name")
	prefix := flag.String("prefix", "", "Optional prefix filter")
	maxKeys := flag.Int("max-keys", 1000, "Max objects per request (pagination size)")

	workers := flag.Int("workers", 4, "Number of parallel workers")
	verbose := flag.Bool("verbose", false, "Show detailed per-file/per-object output instead of only summary")

	maxAttempts := flag.Int("max-attempts", 5, "Maximum retry attempts for AWS SDK operations")
	retryMaxBackoffMs := flag.Int("retry-max-backoff-ms", 5000, "Maximum retry backoff in milliseconds")

	objectKey := flag.String("key", "", "Object key in S3")
	filePath := flag.String("file", "", "Local file to upload")
	folderPath := flag.String("folder", "", "Local folder to upload recursively")
	keyPrefix := flag.String("key-prefix", "", "Optional S3 key prefix for folder uploads")
	outputPath := flag.String("out", "", "Local output file path for single-object download")
	outputDir := flag.String("out-dir", "", "Local output directory for prefix download")
	dryRun := flag.Bool("dry-run", true, "If true, only show what would be deleted")
	allowEmptyPrefixDelete := flag.Bool("allow-empty-prefix-delete", false, "Allow delete to operate on the bucket root when --prefix is empty")
	flag.Parse()

	endpointValue := flagOrEnv(*endpoint, "S3_ENDPOINT")
	regionValue := flagOrEnv(*region, "AWS_REGION", "AWS_DEFAULT_REGION")
	accessKeyValue := flagOrEnv(*accessKey, "AWS_ACCESS_KEY_ID")
	secretKeyValue := flagOrEnv(*secretKey, "AWS_SECRET_ACCESS_KEY")

	fmt.Println("S3Manager v" + version)

	if endpointValue == "" || accessKeyValue == "" || secretKeyValue == "" || *bucket == "" {
		log.Fatal("endpoint, bucket, and credentials are required (use flags or env: S3_ENDPOINT, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION)")
	}
	if *workers < 1 {
		log.Fatal("--workers must be at least 1")
	}
	if *maxAttempts < 1 {
		log.Fatal("--max-attempts must be at least 1")
	}
	if *retryMaxBackoffMs < 0 {
		log.Fatal("--retry-max-backoff-ms must be 0 or higher")
	}

	ctx := context.Background()

	client, err := s3pkg.NewClient(ctx, s3pkg.ClientConfig{
		Endpoint:          endpointValue,
		Region:            regionValue,
		AccessKey:         accessKeyValue,
		SecretKey:         secretKeyValue,
		UsePathStyle:      true,
		MaxAttempts:       *maxAttempts,
		RetryMaxBackoffMs: *retryMaxBackoffMs,
	})
	if err != nil {
		log.Fatalf("failed to create S3 client: %v", err)
	}

	switch *action {
	case "list":
		result, err := enginepkg.ListObjects(ctx, client, *bucket, *prefix, int32(*maxKeys))
		if err != nil {
			log.Fatalf("list failed: %v", err)
		}
		enginepkg.PrintListResult(result)

	case "upload":
		if *filePath == "" || *objectKey == "" {
			log.Fatal("for upload, both --file and --key are required")
		}
		result, err := s3pkg.UploadFile(ctx, client, *bucket, *filePath, *objectKey)
		if err != nil {
			log.Fatalf("upload failed: %v", err)
		}
		s3pkg.PrintUploadResult(result)

	case "upload-folder":
		if *folderPath == "" {
			log.Fatal("for upload-folder, --folder is required")
		}
		result, err := enginepkg.UploadFolder(ctx, client, *bucket, *folderPath, *keyPrefix, *workers, *verbose)
		if err != nil {
			log.Fatalf("upload-folder failed: %v", err)
		}
		enginepkg.PrintUploadFolderResult(result)

	case "download":
		if *objectKey == "" || *outputPath == "" {
			log.Fatal("for download, both --key and --out are required")
		}
		result, err := s3pkg.DownloadFile(ctx, client, *bucket, *objectKey, *outputPath)
		if err != nil {
			log.Fatalf("download failed: %v", err)
		}
		s3pkg.PrintDownloadResult(result)

	case "download-prefix":
		if *outputDir == "" {
			log.Fatal("for download-prefix, --out-dir is required")
		}
		result, err := enginepkg.DownloadPrefix(ctx, client, *bucket, *prefix, *outputDir, int32(*maxKeys), *workers, *verbose)
		if err != nil {
			log.Fatalf("download-prefix failed: %v", err)
		}
		enginepkg.PrintDownloadPrefixResult(result)

	case "head":
		if *objectKey == "" {
			log.Fatal("for head, --key is required")
		}
		result, err := s3pkg.HeadObject(ctx, client, *bucket, *objectKey)
		if err != nil {
			log.Fatalf("head failed: %v", err)
		}
		s3pkg.PrintHeadResult(result)

	case "delete":
		if *prefix == "" && !*allowEmptyPrefixDelete {
			log.Fatal("for delete at bucket root, set --allow-empty-prefix-delete=true")
		}

		result, err := enginepkg.DeletePrefix(ctx, client, *bucket, *prefix, int32(*maxKeys), *dryRun, *workers, *verbose, *allowEmptyPrefixDelete)
		if err != nil {
			log.Fatalf("delete failed: %v", err)
		}
		enginepkg.PrintDeleteResult(result)

	default:
		log.Fatalf("unknown action: %s", *action)
	}
}
