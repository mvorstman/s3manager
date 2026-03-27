package main

import (
	"context"
	"fmt"
	"log"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// deleteObjectsByPrefix finds objects by prefix and either:
// - prints what would be deleted (dry-run)
// - actually deletes them (dry-run=false)
func deleteObjectsByPrefix(ctx context.Context, client *s3.Client, bucket, prefix string, maxKeys int32, dryRun bool) {
	if prefix == "" {
		log.Fatal("for delete, --prefix is required as a safety measure")
	}

	var continuationToken *string

	pageNumber := 0
	totalMatched := 0
	totalDeleted := 0

	fmt.Println("Delete operation starting...")
	fmt.Printf("  Bucket: %s\n", bucket)
	fmt.Printf("  Prefix: %s\n", prefix)
	fmt.Printf("  Dry-run: %v\n", dryRun)
	fmt.Printf("  MaxKeys: %d\n", maxKeys)

	for {
		pageNumber++

		input := &s3.ListObjectsV2Input{
			Bucket:            aws.String(bucket),
			Prefix:            aws.String(prefix),
			ContinuationToken: continuationToken,
			MaxKeys:           aws.Int32(maxKeys),
		}

		resp, err := client.ListObjectsV2(ctx, input)
		if err != nil {
			log.Fatalf("delete scan failed on page %d: %v", pageNumber, err)
		}

		fmt.Printf("\nPage %d\n", pageNumber)
		fmt.Printf("  KeyCount: %d\n", aws.ToInt32(resp.KeyCount))
		fmt.Printf("  IsTruncated: %v\n", aws.ToBool(resp.IsTruncated))

		for _, obj := range resp.Contents {
			key := aws.ToString(obj.Key)
			totalMatched++

			if dryRun {
				fmt.Printf("DRY-RUN would delete: %s\t%d\n", key, obj.Size)
				continue
			}

			_, err := client.DeleteObject(ctx, &s3.DeleteObjectInput{
				Bucket: aws.String(bucket),
				Key:    aws.String(key),
			})
			if err != nil {
				log.Fatalf("failed to delete object %s: %v", key, err)
			}

			totalDeleted++
			fmt.Printf("Deleted: %s\n", key)
		}

		if !aws.ToBool(resp.IsTruncated) {
			break
		}

		continuationToken = resp.NextContinuationToken
	}

	fmt.Printf("\nDelete scan complete.\n")
	fmt.Printf("  Total matched: %d\n", totalMatched)

	if dryRun {
		fmt.Printf("  Total deleted: 0 (dry-run)\n")
	} else {
		fmt.Printf("  Total deleted: %d\n", totalDeleted)
	}
}