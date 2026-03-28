package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type downloadJob struct {
	objectKey  string
	outputPath string
	size       int64
}

func downloadPrefix(ctx context.Context, client *s3.Client, bucket, prefix, outputDir string, maxKeys int32, workers int, verbose bool) {
	startTime := time.Now()

	fmt.Println("Prefix download starting...")
	fmt.Printf("  Bucket: %s\n", bucket)
	fmt.Printf("  Prefix: %s\n", prefix)
	fmt.Printf("  Output dir: %s\n", outputDir)
	fmt.Printf("  MaxKeys: %d\n", maxKeys)
	fmt.Printf("  Workers: %d\n", workers)
	fmt.Printf("  Verbose: %v\n", verbose)

	cleanPrefix := strings.ReplaceAll(prefix, "\\", "/")
	cleanPrefix = strings.TrimPrefix(cleanPrefix, "/")

	outRoot := filepath.Clean(outputDir)

	// ---- First scan: collect jobs ----
	var continuationToken *string
	var jobs []downloadJob
	var totalDiscoveredBytes int64
	pageNumber := 0

	for {
		pageNumber++

		resp, err := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:            aws.String(bucket),
			Prefix:            aws.String(cleanPrefix),
			ContinuationToken: continuationToken,
			MaxKeys:           aws.Int32(maxKeys),
		})
		if err != nil {
			log.Fatalf("download scan failed on page %d: %v", pageNumber, err)
		}

		if verbose {
			fmt.Printf("\nScan page %d\n", pageNumber)
			fmt.Printf("  KeyCount: %d\n", aws.ToInt32(resp.KeyCount))
			fmt.Printf("  IsTruncated: %v\n", aws.ToBool(resp.IsTruncated))
		}

		for _, obj := range resp.Contents {
			key := aws.ToString(obj.Key)

			// Skip objects that exactly equal the prefix (folder placeholder style)
			if key == cleanPrefix || strings.HasSuffix(key, "/") {
				continue
			}

			relativeKey := strings.TrimPrefix(key, cleanPrefix)
			relativeKey = strings.TrimPrefix(relativeKey, "/")

			// Safety: if trimming produced empty path, skip
			if relativeKey == "" {
				continue
			}

			// Convert S3-style key path to local filesystem path
			relativePath := filepath.FromSlash(relativeKey)
			localPath := filepath.Join(outRoot, relativePath)

			size := aws.ToInt64(obj.Size)
			totalDiscoveredBytes += size

			jobs = append(jobs, downloadJob{
				objectKey:  key,
				outputPath: localPath,
				size:       size,
			})

			if verbose {
				fmt.Printf("Queued for download: %s -> %s\n", key, localPath)
			}
		}

		if !aws.ToBool(resp.IsTruncated) {
			break
		}

		continuationToken = resp.NextContinuationToken
	}

	totalJobs := int64(len(jobs))

	fmt.Printf("\nScan complete.\n")
	fmt.Printf("  Files discovered: %d\n", totalJobs)
	fmt.Printf("  Total size: %.2f MB\n", float64(totalDiscoveredBytes)/(1024*1024))

	if totalJobs == 0 {
		fmt.Println("Nothing to download.")
		return
	}

	// ---- Worker pool ----
	jobCh := make(chan downloadJob, workers*2)
	errCh := make(chan error, 1)

	var workerWG sync.WaitGroup
	var downloadedFiles int64
	var downloadedBytes int64

	progressInterval := int64(100)

	for workerID := 1; workerID <= workers; workerID++ {
		workerWG.Add(1)

		go func(id int) {
			defer workerWG.Done()

			for job := range jobCh {
				err := downloadSingleFile(ctx, client, bucket, job)
				if err != nil {
					select {
					case errCh <- fmt.Errorf("worker %d failed on %s: %w", id, job.objectKey, err):
					default:
					}
					return
				}

				newFiles := atomic.AddInt64(&downloadedFiles, 1)
				newBytes := atomic.AddInt64(&downloadedBytes, job.size)

				if verbose {
					fmt.Printf("[worker %d] Downloaded: %s -> %s\n", id, job.objectKey, job.outputPath)
				} else {
					if newFiles%progressInterval == 0 || newFiles == totalJobs {
						percent := float64(newFiles) / float64(totalJobs) * 100
						fmt.Printf("[progress] %d/%d files (%.1f%%) - %.2f MB\n",
							newFiles,
							totalJobs,
							percent,
							float64(newBytes)/(1024*1024),
						)
					}
				}
			}
		}(workerID)
	}

	go func() {
		defer close(jobCh)
		for _, job := range jobs {
			jobCh <- job
		}
	}()

	doneCh := make(chan struct{})
	go func() {
		workerWG.Wait()
		close(doneCh)
	}()

	select {
	case err := <-errCh:
		log.Fatalf("download-prefix failed: %v", err)
	case <-doneCh:
	}

	duration := time.Since(startTime)
	mb := float64(downloadedBytes) / (1024 * 1024)

	var speed float64
	if duration.Seconds() > 0 {
		speed = mb / duration.Seconds()
	}

	fmt.Println("\nDownload completed.")
	fmt.Printf("  Files: %d\n", downloadedFiles)
	fmt.Printf("  Size: %.2f MB\n", mb)
	fmt.Printf("  Duration: %.2fs\n", duration.Seconds())
	fmt.Printf("  Throughput: %.2f MB/s\n", speed)
}

func downloadSingleFile(ctx context.Context, client *s3.Client, bucket string, job downloadJob) error {
	resp, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(job.objectKey),
	})
	if err != nil {
		return fmt.Errorf("GetObject failed: %w", err)
	}
	defer resp.Body.Close()

	outputDir := filepath.Dir(job.outputPath)
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return fmt.Errorf("failed to create output directory %s: %w", outputDir, err)
	}

	outFile, err := os.Create(job.outputPath)
	if err != nil {
		return fmt.Errorf("failed to create output file %s: %w", job.outputPath, err)
	}
	defer outFile.Close()

	if _, err := io.Copy(outFile, resp.Body); err != nil {
		return fmt.Errorf("failed to write downloaded data to %s: %w", job.outputPath, err)
	}

	return nil
}