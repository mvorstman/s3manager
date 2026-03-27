package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func downloadFile(ctx context.Context, client *s3.Client, bucket, objectKey, outputPath string) {
	fmt.Println("Downloading file...")
	fmt.Printf("  Bucket: %s\n", bucket)
	fmt.Printf("  Object key: %s\n", objectKey)
	fmt.Printf("  Output file: %s\n", outputPath)

	// Request the object from S3
	resp, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(objectKey),
	})
	if err != nil {
		log.Fatalf("download failed: %v", err)
	}
	defer resp.Body.Close()

	// Ensure the target folder exists
	outputDir := filepath.Dir(outputPath)
	err = os.MkdirAll(outputDir, 0755)
	if err != nil {
		log.Fatalf("failed to create output directory %s: %v", outputDir, err)
	}

	// Create or overwrite the local output file
	outFile, err := os.Create(outputPath)
	if err != nil {
		log.Fatalf("failed to create output file %s: %v", outputPath, err)
	}
	defer outFile.Close()

	// Copy the S3 object body into the local file
	bytesWritten, err := io.Copy(outFile, resp.Body)
	if err != nil {
		log.Fatalf("failed to write downloaded data to %s: %v", outputPath, err)
	}

	fmt.Println("Download completed successfully.")
	fmt.Printf("  Bytes written: %d\n", bytesWritten)
}