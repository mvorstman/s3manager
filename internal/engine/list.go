package engine

import (
	"context"
	"fmt"

	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"

	s3pkg "s3manager/internal/s3"
)

type ListResult struct {
	Bucket       string
	Prefix       string
	Pages        int
	TotalObjects int
}

func ListObjects(
	ctx context.Context,
	client *awss3.Client,
	bucket, prefix string,
	maxKeys int32,
) (ListResult, error) {
	s3Result, err := s3pkg.ListObjects(ctx, client, bucket, prefix, maxKeys)
	if err != nil {
		return ListResult{}, err
	}

	return ListResult{
		Bucket:       bucket,
		Prefix:       prefix,
		Pages:        s3Result.Pages,
		TotalObjects: s3Result.TotalObjects,
	}, nil
}

func PrintListResult(result ListResult) {
	fmt.Println("List summary")
	fmt.Printf("  Bucket: %s\n", result.Bucket)
	fmt.Printf("  Prefix: %s\n", result.Prefix)
	fmt.Printf("  Pages: %d\n", result.Pages)
	fmt.Printf("  Total objects: %d\n", result.TotalObjects)
}
