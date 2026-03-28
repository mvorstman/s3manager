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
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type uploadJob struct {
	localPath string
	objectKey string
	size      int64
}

func uploadFolder(ctx context.Context, client *s3.Client, bucket, folderPath, keyPrefix string, workers int) {
	startTime := time.Now()

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

	// ---- Collect jobs ----
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
			return err
		}

		rel, err := filepath.Rel(rootPath, path)
		if err != nil {
			return err
		}

		key := strings.ReplaceAll(rel, "\\", "/")
		key = normalizedPrefix + key

		jobs = append(jobs, uploadJob{
			localPath: path,
			objectKey: key,
			size:      info.Size(),
		})

		return nil
	})
	if err != nil {
		log.Fatalf("scan failed: %v", err)
	}

	totalJobs := int64(len(jobs))

	fmt.Printf("  Files discovered: %d\n", totalJobs)

	if totalJobs == 0 {
		fmt.Println("Nothing to upload.")
		return
	}

	jobCh := make(chan uploadJob)
	errCh := make(chan error, 1)

	var wg sync.WaitGroup

	var uploadedFiles int64
	var uploadedBytes int64

	progressInterval := int64(100) // update every 100 files

	// ---- Workers ----
	for i := 1; i <= workers; i++ {
		wg.Add(1)

		go func(workerID int) {
			defer wg.Done()

			for job := range jobCh {

				err := uploadSingleFile(ctx, client, bucket, job)
				if err != nil {
					select {
					case errCh <- err:
					default:
					}
					return
				}

				newCount := atomic.AddInt64(&uploadedFiles, 1)
				newBytes := atomic.AddInt64(&uploadedBytes, job.size)

				// ---- Progress reporting ----
				if newCount%progressInterval == 0 || newCount == totalJobs {
					percent := float64(newCount) / float64(totalJobs) * 100
					fmt.Printf("[progress] %d/%d files (%.1f%%) - %.2f MB\n",
						newCount,
						totalJobs,
						percent,
						float64(newBytes)/(1024*1024),
					)
				}
			}
		}(i)
	}

	// ---- Feed jobs ----
	go func() {
		defer close(jobCh)
		for _, job := range jobs {
			jobCh <- job
		}
	}()

	// ---- Wait ----
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case err := <-errCh:
		log.Fatalf("upload failed: %v", err)
	case <-done:
	}

	// ---- Final summary ----
	duration := time.Since(startTime)
	mb := float64(uploadedBytes) / (1024 * 1024)
	speed := mb / duration.Seconds()

	fmt.Println("\nUpload completed.")
	fmt.Printf("  Files: %d\n", uploadedFiles)
	fmt.Printf("  Size: %.2f MB\n", mb)
	fmt.Printf("  Duration: %.2fs\n", duration.Seconds())
	fmt.Printf("  Throughput: %.2f MB/s\n", speed)
}

func uploadSingleFile(ctx context.Context, client *s3.Client, bucket string, job uploadJob) error {
	file, err := os.Open(job.localPath)
	if err != nil {
		return err
	}
	defer file.Close()

	_, err = client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(job.objectKey),
		Body:   file,
	})

	return err
}