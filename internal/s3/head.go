package s3

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
)

type HeadResult struct {
	Bucket       string
	Key          string
	Size         int64
	ETag         string
	ContentType  string
	LastModified *time.Time
	StorageClass string
	VersionID    string
	Metadata     map[string]string
}

func HeadObject(ctx context.Context, client *awss3.Client, bucket, objectKey string) (HeadResult, error) {
	resp, err := client.HeadObject(ctx, &awss3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(objectKey),
	})
	if err != nil {
		return HeadResult{}, fmt.Errorf("head object failed: %w", err)
	}

	result := HeadResult{
		Bucket:      bucket,
		Key:         objectKey,
		Size:        aws.ToInt64(resp.ContentLength),
		ETag:        aws.ToString(resp.ETag),
		ContentType: aws.ToString(resp.ContentType),
		VersionID:   aws.ToString(resp.VersionId),
		Metadata:    make(map[string]string, len(resp.Metadata)),
	}

	if resp.LastModified != nil {
		t := *resp.LastModified
		result.LastModified = &t
	}

	if resp.StorageClass != "" {
		result.StorageClass = string(resp.StorageClass)
	}

	for k, v := range resp.Metadata {
		result.Metadata[k] = v
	}

	return result, nil
}

func PrintHeadResult(result HeadResult) {
	fmt.Println("Reading object metadata...")
	fmt.Printf("  Bucket: %s\n", result.Bucket)
	fmt.Printf("  Object key: %s\n", result.Key)

	fmt.Println("\nObject metadata:")
	fmt.Printf("  Key: %s\n", result.Key)
	fmt.Printf("  Size: %d bytes\n", result.Size)
	fmt.Printf("  ETag: %s\n", result.ETag)
	fmt.Printf("  Content-Type: %s\n", result.ContentType)

	if result.LastModified != nil {
		fmt.Printf("  Last-Modified: %s\n", result.LastModified.Format("2006-01-02 15:04:05 MST"))
	}

	if result.StorageClass != "" {
		fmt.Printf("  Storage-Class: %s\n", result.StorageClass)
	}

	if result.VersionID != "" {
		fmt.Printf("  Version-Id: %s\n", result.VersionID)
	}

	if len(result.Metadata) > 0 {
		fmt.Println("  User metadata:")
		keys := make([]string, 0, len(result.Metadata))
		for k := range result.Metadata {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			fmt.Printf("    %s: %s\n", k, result.Metadata[k])
		}
	} else {
		fmt.Println("  User metadata: none")
	}
}
