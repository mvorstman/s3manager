package engine

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"

	s3pkg "s3manager/internal/s3"
)

type UploadFolderResult struct {
	Bucket    string
	Folder    string
	KeyPrefix string
	Uploaded  int64
	Failed    int64
	TotalBytes int64
	Duration  time.Duration
}

func UploadFolder(
	ctx context.Context,
	client *awss3.Client,
	bucket, folderPath, keyPrefix string,
	workers int,
	verbose bool,
) (UploadFolderResult, error) {
	_ = workers // sequential baseline first

	start := time.Now()
	result := UploadFolderResult{
		Bucket:    bucket,
		Folder:    folderPath,
		KeyPrefix: keyPrefix,
	}

	root, err := filepath.Abs(folderPath)
	if err != nil {
		return result, fmt.Errorf("resolve folder path %s: %w", folderPath, err)
	}

	err = filepath.Walk(root, func(path string, info os.FileInfo, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if info.IsDir() {
			return nil
		}

		relPath, err := filepath.Rel(root, path)
		if err != nil {
			return fmt.Errorf("build relative path for %s: %w", path, err)
		}

		key := filepath.ToSlash(relPath)
		if keyPrefix != "" {
			cleanPrefix := strings.TrimSuffix(keyPrefix, "/")
			key = cleanPrefix + "/" + key
		}

		if verbose {
			fmt.Printf("Upload %s -> %s\n", path, key)
		}

		uploadResult, err := s3pkg.UploadFile(ctx, client, bucket, path, key)
		if err != nil {
			result.Failed++
			if verbose {
				fmt.Printf("Upload failed for %s: %v\n", path, err)
			}
			return nil
		}

		result.Uploaded++
		result.TotalBytes += uploadResult.Size
		return nil
	})
	if err != nil {
		return result, fmt.Errorf("walk folder %s: %w", folderPath, err)
	}

	result.Duration = time.Since(start)
	return result, nil
}

func PrintUploadFolderResult(result UploadFolderResult) {
	fmt.Println("Upload-folder summary")
	fmt.Printf("  Bucket: %s\n", result.Bucket)
	fmt.Printf("  Folder: %s\n", result.Folder)
	fmt.Printf("  Key prefix: %s\n", result.KeyPrefix)
	fmt.Printf("  Uploaded: %d\n", result.Uploaded)
	fmt.Printf("  Failed: %d\n", result.Failed)
	fmt.Printf("  Total bytes: %d\n", result.TotalBytes)
	fmt.Printf("  Duration: %s\n", result.Duration.Round(time.Millisecond))
}