package main

import (
	"context"
	"fmt"
	"log"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func listAllObjects(ctx context.Context, client *s3.Client, bucket, prefix string, maxKeys int32) {
	var continuationToken *string

	pageNumber := 0
	totalObjects := 0

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
			log.Fatalf("list objects failed on page %d: %v", pageNumber, err)
		}

		fmt.Printf("Page %d\n", pageNumber)
		fmt.Printf("  KeyCount: %d\n", aws.ToInt32(resp.KeyCount))
		fmt.Printf("  MaxKeys: %d\n", maxKeys)
		fmt.Printf("  IsTruncated: %v\n", aws.ToBool(resp.IsTruncated))

		for _, obj := range resp.Contents {
			totalObjects++
			fmt.Printf("%s\t%d\n", aws.ToString(obj.Key), obj.Size)
		}

		if !aws.ToBool(resp.IsTruncated) {
			break
		}

		continuationToken = resp.NextContinuationToken
	}

	fmt.Printf("\nDone. Total objects listed: %d\n", totalObjects)
}