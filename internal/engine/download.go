package engine

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"

	s3pkg "s3manager/internal/s3"
)

type DownloadPrefixResult struct {
	Bucket     string
	Prefix     string
	OutputDir  string
	Downloaded int64
	Failed     int64
	TotalBytes int64
	Duration   time.Duration
}

func DownloadPrefix(
	ctx context.Context,
	client *awss3.Client,
	bucket, prefix, outputDir string,
	maxKeys int32,
	workers int,
	verbose bool,
) (DownloadPrefixResult, error) {
	_ = workers // kept for the later concurrent version

	start := time.Now()
	result := DownloadPrefixResult{
		Bucket:    bucket,
		Prefix:    prefix,
		OutputDir: outputDir,
	}

	listResult, err := s3pkg.ListObjects(ctx, client, bucket, prefix, maxKeys)
	if err != nil {
		return result, fmt.Errorf("list objects for download-prefix failed: %w", err)
	}
	_ = listResult // sequential baseline does not use page/object counters yet

	var continuationToken *string
	for {
		resp, err := client.ListObjectsV2(ctx, &awss3.ListObjectsV2Input{
			Bucket:            &bucket,
			Prefix:            &prefix,
			ContinuationToken: continuationToken,
			MaxKeys:           &maxKeys,
		})
		if err != nil {
			return result, fmt.Errorf("list objects page for download-prefix failed: %w", err)
		}

		for _, obj := range resp.Contents {
			key := ""
			if obj.Key != nil {
				key = *obj.Key
			}
			if key == "" || strings.HasSuffix(key, "/") {
				continue
			}

			localPath := localPathForObject(outputDir, prefix, key)
			if verbose {
				fmt.Printf("Download %s -> %s\n", key, localPath)
			}

			downloadResult, err := s3pkg.DownloadFile(ctx, client, bucket, key, localPath)
			if err != nil {
				result.Failed++
				if verbose {
					fmt.Printf("Download failed for %s: %v\n", key, err)
				}
				continue
			}

			result.Downloaded++
			result.TotalBytes += downloadResult.Size
		}

		if resp.IsTruncated == nil || !*resp.IsTruncated {
			break
		}
		continuationToken = resp.NextContinuationToken
	}

	result.Duration = time.Since(start)
	return result, nil
}

func localPathForObject(outputDir, prefix, key string) string {
	relative := strings.TrimPrefix(key, prefix)
	relative = strings.TrimLeft(relative, "/")
	if relative == "" {
		relative = filepath.Base(key)
	}
	return filepath.Join(outputDir, relative)
}

func PrintDownloadPrefixResult(result DownloadPrefixResult) {
	fmt.Println("Download-prefix summary")
	fmt.Printf("  Bucket: %s\n", result.Bucket)
	fmt.Printf("  Prefix: %s\n", result.Prefix)
	fmt.Printf("  Output dir: %s\n", result.OutputDir)
	fmt.Printf("  Downloaded: %d\n", result.Downloaded)
	fmt.Printf("  Failed: %d\n", result.Failed)
	fmt.Printf("  Total bytes: %d\n", result.TotalBytes)
	fmt.Printf("  Duration: %s\n", result.Duration.Round(time.Millisecond))
}