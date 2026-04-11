package engine

import (
	"context"
	"fmt"
	"sync"
	"time"

	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"

	s3pkg "s3manager/internal/s3"
)

func uploadWorker(
	ctx context.Context,
	client *awss3.Client,
	bucket string,
	jobs <-chan UploadJob,
	results chan<- UploadFileResult,
) {
	for job := range jobs {
		fileStart := time.Now()

		uploadResult, err := s3pkg.UploadFile(ctx, client, bucket, job.LocalPath, job.Key)
		if err != nil {
			results <- UploadFileResult{
				LocalPath: job.LocalPath,
				Key:       job.Key,
				Bytes:     job.Size,
				Duration:  time.Since(fileStart),
				Err:       err,
			}
			continue
		}

		results <- UploadFileResult{
			LocalPath: job.LocalPath,
			Key:       job.Key,
			Bytes:     uploadResult.Size,
			Duration:  time.Since(fileStart),
			Err:       nil,
		}
	}
}

func UploadFolder(
	ctx context.Context,
	client *awss3.Client,
	bucket, folderPath, keyPrefix string,
	workers int,
	verbose bool,
) (UploadFolderResult, error) {
	start := time.Now()
	result := UploadFolderResult{}

	jobs, err := BuildUploadJobs(folderPath, keyPrefix)
	if err != nil {
		return result, err
	}

	result.TotalFiles = len(jobs)
	for _, job := range jobs {
		result.TotalBytes += job.Size
	}

	jobsCh := make(chan UploadJob)
	resultsCh := make(chan UploadFileResult)

	if workers < 1 {
		workers = 1
	}

	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			uploadWorker(ctx, client, bucket, jobsCh, resultsCh)
		}()
	}

	go func() {
		defer close(jobsCh)
		for _, job := range jobs {
			if verbose {
				fmt.Printf("Queue %s -> %s\n", job.LocalPath, job.Key)
			}
			jobsCh <- job
		}
	}()

	go func() {
		wg.Wait()
		close(resultsCh)
	}()

	for fileResult := range resultsCh {
		if fileResult.Err != nil {
			result.FailedFiles++
			result.FailedBytes += fileResult.Bytes
		} else {
			result.SuccessfulFiles++
			result.UploadedBytes += fileResult.Bytes
		}

		result.Files = append(result.Files, fileResult)
	}

	result.Duration = time.Since(start)
	return result, nil
}

func PrintUploadFolderResult(result UploadFolderResult) {
	fmt.Println("Upload-folder summary")
	fmt.Printf("  Total files: %d\n", result.TotalFiles)
	fmt.Printf("  Success: %d\n", result.SuccessfulFiles)
	fmt.Printf("  Failed: %d\n", result.FailedFiles)
	fmt.Printf("  Total bytes: %d\n", result.TotalBytes)
	fmt.Printf("  Uploaded bytes: %d\n", result.UploadedBytes)
	fmt.Printf("  Failed bytes: %d\n", result.FailedBytes)
	fmt.Printf("  Duration: %s\n", result.Duration)

	throughputMiB := 0.0
	if result.Duration > 0 {
		throughputMiB = float64(result.UploadedBytes) / 1024.0 / 1024.0 / result.Duration.Seconds()
	}
	fmt.Printf("  Throughput: %.2f MiB/s\n", throughputMiB)
}
