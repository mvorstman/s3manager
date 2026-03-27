package main

import (
	"context"
	"fmt"
	"log"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	types "github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// deleteObjectsByPrefix finds objects by prefix and either:
// - prints what would be deleted (dry-run)
// - deletes them in batches of up to 1000 objects (dry-run=false)
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

		// Build a batch of objects for DeleteObjects
		var batch []types.ObjectIdentifier

		for _, obj := range resp.Contents {
			key := aws.ToString(obj.Key)
			totalMatched++

			if dryRun {
				fmt.Printf("DRY-RUN would delete: %s\t%d\n", key, obj.Size)
				continue
			}

			batch = append(batch, types.ObjectIdentifier{
				Key: aws.String(key),
			})
		}

		// Only call DeleteObjects when not in dry-run mode and batch has items
		if !dryRun && len(batch) > 0 {
			deletedCount := deleteBatch(ctx, client, bucket, batch)
			totalDeleted += deletedCount
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

// deleteBatch deletes up to 1000 objects in one S3 API call.
func deleteBatch(ctx context.Context, client *s3.Client, bucket string, batch []types.ObjectIdentifier) int {
	fmt.Printf("  Deleting batch of %d objects...\n", len(batch))

	resp, err := client.DeleteObjects(ctx, &s3.DeleteObjectsInput{
		Bucket: aws.String(bucket),
		Delete: &types.Delete{
			Objects: batch,
			Quiet:   aws.Bool(false), // false = S3 returns details about deleted objects
		},
	})
	if err != nil {
		log.Fatalf("batch delete failed: %v", err)
	}

	// Report objects S3 says were deleted
	for _, d := range resp.Deleted {
		fmt.Printf("Deleted: %s\n", aws.ToString(d.Key))
	}

	// Report any per-object errors returned by S3
	for _, e := range resp.Errors {
		fmt.Printf("Delete error: key=%s code=%s message=%s\n",
			aws.ToString(e.Key),
			aws.ToString(e.Code),
			aws.ToString(e.Message),
		)
	}

	return len(resp.Deleted)
}