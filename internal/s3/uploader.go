package s3

import (
	"context"
	"fmt"
	"mime"
	"os"
	"path/filepath"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
)

// UploadResult contains the outcome of a single-file upload.
type UploadResult struct {
	Bucket   string
	Key      string
	FilePath string
	Size     int64
	ETag     string
}

// UploadFile uploads a single local file to S3 and returns a structured result.
func UploadFile(ctx context.Context, client *awss3.Client, bucket, filePath, objectKey string) (UploadResult, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return UploadResult{}, fmt.Errorf("open file %s: %w", filePath, err)
	}
	defer file.Close()

	fileInfo, err := file.Stat()
	if err != nil {
		return UploadResult{}, fmt.Errorf("stat file %s: %w", filePath, err)
	}

	contentType := mime.TypeByExtension(filepath.Ext(filePath))
	if contentType == "" {
		contentType = "application/octet-stream"
	}

	input := &awss3.PutObjectInput{
		Bucket:      aws.String(bucket),
		Key:         aws.String(objectKey),
		Body:        file,
		ContentType: aws.String(contentType),
	}

	resp, err := client.PutObject(ctx, input)
	if err != nil {
		return UploadResult{}, fmt.Errorf("upload file %s to s3://%s/%s: %w", filePath, bucket, objectKey, err)
	}

	return UploadResult{
		Bucket:   bucket,
		Key:      objectKey,
		FilePath: filePath,
		Size:     fileInfo.Size(),
		ETag:     aws.ToString(resp.ETag),
	}, nil
}

// PrintUploadResult renders a human-readable upload summary for CLI use.
func PrintUploadResult(result UploadResult) {
	fmt.Println("Upload completed successfully.")
	fmt.Printf("  Local file: %s\n", result.FilePath)
	fmt.Printf("  Bucket: %s\n", result.Bucket)
	fmt.Printf("  Object key: %s\n", result.Key)
	fmt.Printf("  File size: %d bytes\n", result.Size)
	if result.ETag != "" {
		fmt.Printf("  ETag: %s\n", result.ETag)
	}
}
