package engine

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"

	s3pkg "s3manager/internal/s3"
)

type DownloadPrefixResult struct {
	Bucket      string
	Prefix      string
	OutputDir   string
	ListedPages int
	Queued      int64
	Downloaded  int64
	Failed      int64
	TotalBytes  int64
	Duration    time.Duration
}

type downloadJob struct {
	key       string
	localPath string
}

func DownloadPrefix(
	ctx context.Context,
	client *awss3.Client,
	bucket, prefix, outputDir string,
	maxKeys int32,
	workers int,
	verbose bool,
) (DownloadPrefixResult, error) {
	if workers < 1 {
		workers = 1
	}
	if maxKeys < 1 {
		maxKeys = 1000
	}

	start := time.Now()
	result := DownloadPrefixResult{
		Bucket:    bucket,
		Prefix:    prefix,
		OutputDir: outputDir,
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	jobs := make(chan downloadJob, workers*2)
	errCh := make(chan error, workers+1)

	var queued int64
	var downloaded int64
	var failed int64
	var totalBytes int64

	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			for job := range jobs {
				if ctx.Err() != nil {
					return
				}

				if verbose {
					fmt.Printf("Download %s -> %s\n", job.key, job.localPath)
				}

				downloadResult, err := s3pkg.DownloadFile(ctx, client, bucket, job.key, job.localPath)
				if err != nil {
					atomic.AddInt64(&failed, 1)
					if verbose {
						fmt.Printf("Download failed for %s: %v\n", job.key, err)
					}
					continue
				}

				atomic.AddInt64(&downloaded, 1)
				atomic.AddInt64(&totalBytes, downloadResult.Size)
			}
		}(i + 1)
	}

	var continuationToken *string
	for {
		result.ListedPages++

		resp, err := client.ListObjectsV2(ctx, &awss3.ListObjectsV2Input{
			Bucket:            aws.String(bucket),
			Prefix:            aws.String(prefix),
			ContinuationToken: continuationToken,
			MaxKeys:           aws.Int32(maxKeys),
		})
		if err != nil {
			close(jobs)
			wg.Wait()
			return result, fmt.Errorf("list objects for download-prefix failed on page %d: %w", result.ListedPages, err)
		}

		for _, obj := range resp.Contents {
			key := aws.ToString(obj.Key)
			if key == "" || strings.HasSuffix(key, "/") {
				continue
			}

			job := downloadJob{
				key:       key,
				localPath: localPathForObject(outputDir, prefix, key),
			}

			atomic.AddInt64(&queued, 1)

			select {
			case jobs <- job:
			case err := <-errCh:
				close(jobs)
				wg.Wait()
				return result, err
			case <-ctx.Done():
				close(jobs)
				wg.Wait()
				return result, ctx.Err()
			}
		}

		if !aws.ToBool(resp.IsTruncated) {
			break
		}
		continuationToken = resp.NextContinuationToken
	}

	close(jobs)
	wg.Wait()

	select {
	case err := <-errCh:
		return result, err
	default:
	}

	result.Queued = atomic.LoadInt64(&queued)
	result.Downloaded = atomic.LoadInt64(&downloaded)
	result.Failed = atomic.LoadInt64(&failed)
	result.TotalBytes = atomic.LoadInt64(&totalBytes)
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
	fmt.Printf("  Listed pages: %d\n", result.ListedPages)
	fmt.Printf("  Queued: %d\n", result.Queued)
	fmt.Printf("  Downloaded: %d\n", result.Downloaded)
	fmt.Printf("  Failed: %d\n", result.Failed)
	fmt.Printf("  Total bytes: %d\n", result.TotalBytes)
	fmt.Printf("  Duration: %s\n", result.Duration.Round(time.Millisecond))
}
