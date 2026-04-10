package s3

import (
	"context"
	"fmt"
	"sort"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func headObject(ctx context.Context, client *s3.Client, bucket, objectKey string) {
	fmt.Println("Reading object metadata...")
	fmt.Printf("  Bucket: %s\n", bucket)
	fmt.Printf("  Object key: %s\n", objectKey)

	resp, err := client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(objectKey),
	})
	if err != nil {
		fmt.Printf("head object failed: %v\n", err)
		return
	}

	fmt.Println("\nObject metadata:")
	fmt.Printf("  Key: %s\n", objectKey)
	fmt.Printf("  Size: %d bytes\n", aws.ToInt64(resp.ContentLength))
	fmt.Printf("  ETag: %s\n", aws.ToString(resp.ETag))
	fmt.Printf("  Content-Type: %s\n", aws.ToString(resp.ContentType))

	if resp.LastModified != nil {
		fmt.Printf("  Last-Modified: %s\n", resp.LastModified.Format("2006-01-02 15:04:05 MST"))
	}

	if resp.StorageClass != "" {
		fmt.Printf("  Storage-Class: %s\n", resp.StorageClass)
	}

	if aws.ToString(resp.VersionId) != "" {
		fmt.Printf("  Version-Id: %s\n", aws.ToString(resp.VersionId))
	}

	if len(resp.Metadata) > 0 {
		fmt.Println("  User metadata:")
		keys := make([]string, 0, len(resp.Metadata))
		for k := range resp.Metadata {
			keys = append(keys, k)
		}
		sort.Strings(keys)

		for _, k := range keys {
			fmt.Printf("    %s: %s\n", k, resp.Metadata[k])
		}
	} else {
		fmt.Println("  User metadata: none")
	}
}