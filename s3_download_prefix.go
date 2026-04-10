package main

import (
	"context"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"

	enginepkg "s3manager/internal/engine"
)

// downloadPrefix is now a thin compatibility wrapper.
// The real orchestration lives in internal/engine/download.go.
func downloadPrefix(ctx context.Context, client *awss3.Client, bucket, prefix, outputDir string, maxKeys int32, workers int, verbose bool) {
	result, err := enginepkg.DownloadPrefix(ctx, client, bucket, prefix, outputDir, maxKeys, workers, verbose)
	if err != nil {
		panic(err)
	}
	enginepkg.PrintDownloadPrefixResult(result)
}