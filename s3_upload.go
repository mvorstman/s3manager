package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func uploadFile(ctx context.Context, client *s3.Client, bucket, filePath, objectKey string) {
	// Open the local file for reading
	file, err := os.Open(filePath)
	if err != nil {
		log.Fatalf("failed to open file %s: %v", filePath, err)
	}
	defer file.Close()

	// Get file info so we can show size in the output
	fileInfo, err := file.Stat()
	if err != nil {
		log.Fatalf("failed to stat file %s: %v", filePath, err)
	}

	fmt.Println("Uploading file...")
	fmt.Printf("  Local file: %s\n", filePath)
	fmt.Printf("  Bucket: %s\n", bucket)
	fmt.Printf("  Object key: %s\n", objectKey)
	fmt.Printf("  File size: %d bytes\n", fileInfo.Size())

	input := &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(objectKey),
		Body:   file,
	}

	_, err = client.PutObject(ctx, input)
	if err != nil {
		log.Fatalf("upload failed: %v", err)
	}

	fmt.Println("Upload completed successfully.")
}