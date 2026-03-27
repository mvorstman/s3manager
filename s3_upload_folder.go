package main

import (
	"context"
	"fmt"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type uploadJob struct {
	localPath string
	objectKey string
	size      int64
}

func uploadFolder(ctx context.Context, client *s3.Client, bucket, folderPath, keyPrefix string, workers int) {
	fmt.Println("Folder upload starting...")
	fmt.Printf("  Local folder: %s\n", folderPath)
	fmt.Printf("  Bucket: %s\n", bucket)
	fmt.Printf("  Key prefix: %s\n", keyPrefix)
	fmt.Printf("  Workers: %d\n", workers)

	rootPath := filepath.Clean(folderPath)

	normalizedPrefix := strings.ReplaceAll(keyPrefix, "\\", "/")
	if normalizedPrefix != "" && !strings.HasSuffix(normalizedPrefix, "/") {
		normalizedPrefix += "/"
	}

	// First collect all upload jobs
	var jobs []uploadJob

	err := filepath.WalkDir(rootPath, func(path string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}

		if d.IsDir() {
			return nil
		}

		info, err := d.Info()
		if err != nil {
			return fmt.Errorf("failed to read info for %s: %w", path, err)
		}

		relativePath, err := filepath.Rel(rootPath, path)
		if err != nil {
			return fmt.Errorf("failed to compute relative path for %s: %w", path, err)
		}

		objectKey := strings.ReplaceAll(relativePath, "\\", "/")
		objectKey = normalizedPrefix + objectKey

		jobs = append(jobs, uploadJob{
			localPath: path,
			objectKey: objectKey,
			size:      info.Size(),
		})

		return nil
	})
	if err != nil {
		log.Fatalf("failed to scan folder: %v", err)
	}

	fmt.Printf("  Files discovered: %d\n", len(jobs))

	if len(jobs) == 0 {
		fmt.Println("No files found. Nothing to upload.")
		return
	}

	jobCh := make(chan uploadJob)
	errCh := make(chan error, 1)

	var wg sync.WaitGroup
	var totalFilesUploaded int64
	var totalBytesUploaded int64

	// Start worker goroutines
	for workerID := 1; workerID <= workers; workerID++ {
		wg.Add(1)

		go func(id int) {
			defer wg.Done()

			for job := range jobCh {
				select {
				case <-ctx.Done():
					return
				default:
				}

				err := uploadSingleFile(ctx, client, bucket, job)
				if err != nil {
					select {
					case errCh <- fmt.Errorf("worker %d failed on %s: %w", id, job.localPath, err):
					default:
					}
					return
				}

				atomic.AddInt64(&totalFilesUploaded, 1)
				atomic.AddInt64(&totalBytesUploaded, job.size)

				fmt.Printf("[worker %d] Uploaded: %s -> %s\n", id, job.localPath, job.objectKey)
			}
		}(workerID)
	}

	// Feed jobs to workers
	go func() {
		defer close(jobCh)

		for _, job := range jobs {
			select {
			case <-ctx.Done():
				return
			case jobCh <- job:
			}
		}
	}()

	// Wait for workers to finish
	doneCh := make(chan struct{})
	go func() {
		wg.Wait()
		close(doneCh)
	}()

	select {
	case err := <-errCh:
		log.Fatalf("folder upload failed: %v", err)
	case <-doneCh:
	}

	fmt.Println("Folder upload completed successfully.")
	fmt.Printf("  Total files uploaded: %d\n", totalFilesUploaded)
	fmt.Printf("  Total bytes uploaded: %d\n", totalBytesUploaded)
}

func uploadSingleFile(ctx context.Context, client *s3.Client, bucket string, job uploadJob) error {
	file, err := os.Open(job.localPath)
	if err != nil {
		return fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	_, err = client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(job.objectKey),
		Body:   file,
	})
	if err != nil {
		return fmt.Errorf("upload failed: %w", err)
	}

	return nil
}