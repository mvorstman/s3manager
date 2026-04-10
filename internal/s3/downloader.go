package s3

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
)

type DownloadResult struct {
	Bucket  string
	Key     string
	OutPath string
	Size    int64
}

func DownloadFile(ctx context.Context, client *awss3.Client, bucket, objectKey, outputPath string) (DownloadResult, error) {
	resp, err := client.GetObject(ctx, &awss3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(objectKey),
	})
	if err != nil {
		return DownloadResult{}, fmt.Errorf("get object s3://%s/%s: %w", bucket, objectKey, err)
	}
	defer resp.Body.Close()

	outputDir := filepath.Dir(outputPath)
	if err := os.MkdirAll(outputDir, 0o755); err != nil {
		return DownloadResult{}, fmt.Errorf("create output directory %s: %w", outputDir, err)
	}

	outFile, err := os.Create(outputPath)
	if err != nil {
		return DownloadResult{}, fmt.Errorf("create output file %s: %w", outputPath, err)
	}

	written, copyErr := io.Copy(outFile, resp.Body)
	closeErr := outFile.Close()
	if copyErr != nil {
		return DownloadResult{}, fmt.Errorf("write downloaded data to %s: %w", outputPath, copyErr)
	}
	if closeErr != nil {
		return DownloadResult{}, fmt.Errorf("close output file %s: %w", outputPath, closeErr)
	}

	return DownloadResult{
		Bucket:  bucket,
		Key:     objectKey,
		OutPath: outputPath,
		Size:    written,
	}, nil
}

func PrintDownloadResult(result DownloadResult) {
	fmt.Println("Download completed successfully.")
	fmt.Printf("  Bucket: %s\n", result.Bucket)
	fmt.Printf("  Object key: %s\n", result.Key)
	fmt.Printf("  Output file: %s\n", result.OutPath)
	fmt.Printf("  Bytes written: %d\n", result.Size)
}
