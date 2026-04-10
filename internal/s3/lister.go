package s3

import (
	"context"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
)

// ListResult contains the aggregated outcome of a list operation.
type ListResult struct {
	Pages        int
	TotalObjects int
}

// ListObjects lists all matching objects and writes human-readable output to stdout.
func ListObjects(ctx context.Context, client *awss3.Client, bucket, prefix string, maxKeys int32) (ListResult, error) {
	var continuationToken *string
	result := ListResult{}

	for {
		result.Pages++

		input := &awss3.ListObjectsV2Input{
			Bucket:            aws.String(bucket),
			Prefix:            aws.String(prefix),
			ContinuationToken: continuationToken,
			MaxKeys:           aws.Int32(maxKeys),
		}

		resp, err := client.ListObjectsV2(ctx, input)
		if err != nil {
			return result, fmt.Errorf("list objects failed on page %d: %w", result.Pages, err)
		}

		fmt.Printf("Page %d\n", result.Pages)
		fmt.Printf("  KeyCount: %d\n", aws.ToInt32(resp.KeyCount))
		fmt.Printf("  MaxKeys: %d\n", maxKeys)
		fmt.Printf("  IsTruncated: %v\n", aws.ToBool(resp.IsTruncated))

	for _, obj := range resp.Contents {
		result.TotalObjects++
		key := strings.TrimRight(aws.ToString(obj.Key), "\x00")
		fmt.Printf("%s\t%d\n", key, aws.ToInt64(obj.Size))
}

		if !aws.ToBool(resp.IsTruncated) {
			break
		}

		continuationToken = resp.NextContinuationToken
	}

	fmt.Printf("\nDone. Total objects listed: %d\n", result.TotalObjects)
	return result, nil
}