package main

import (
	"context"
	"flag"
	"fmt"
	"log"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// Version marker so you know where you are in development
const version = "0.3"

// main is the entry point of the program
func main() {
	// ---- CLI FLAGS ----
	// These let you control the program from PowerShell
	endpoint := flag.String("endpoint", "", "S3 endpoint, for example https://s3.example.local")
	region := flag.String("region", "us-east-1", "AWS region")
	accessKey := flag.String("access-key", "", "S3 access key")
	secretKey := flag.String("secret-key", "", "S3 secret key")
	bucket := flag.String("bucket", "", "Bucket name")
	prefix := flag.String("prefix", "", "Optional prefix filter")
	maxKeys := flag.Int("max-keys", 1000, "Max objects per request (pagination size)")

	flag.Parse()

	fmt.Println("S3Manager v" + version)

	// Basic validation: we cannot continue without these required inputs
	if *endpoint == "" || *accessKey == "" || *secretKey == "" || *bucket == "" {
		log.Fatal("endpoint, access-key, secret-key, and bucket are required")
	}

	// Context is used by Go for request lifecycle handling
	// Later this can also be used for cancellation and timeouts
	ctx := context.Background()

	// ---- LOAD AWS SDK CONFIG ----
	// This creates the shared AWS configuration object used by the SDK client
	cfg, err := awsconfig.LoadDefaultConfig(
		ctx,

		// Region is still needed for signing, even with S3-compatible endpoints
		awsconfig.WithRegion(*region),

		// We use static credentials provided on the command line
		awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(*accessKey, *secretKey, ""),
		),
	)
	if err != nil {
		log.Fatalf("failed to load AWS config: %v", err)
	}

	// ---- CREATE S3 CLIENT ----
	// This is the object that will make real S3 API calls
	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		// Many S3-compatible systems, including StorageGRID, often work best with path-style requests
		// Example: https://endpoint/bucket/object
		o.UsePathStyle = true

		// Override the normal AWS S3 endpoint with your own S3-compatible endpoint
		o.BaseEndpoint = aws.String(*endpoint)
	})

	// ---- START LISTING OBJECTS ----
	listAllObjects(ctx, client, *bucket, *prefix, int32(*maxKeys))
}

// listAllObjects retrieves all objects in a bucket by following pagination until the end
func listAllObjects(ctx context.Context, client *s3.Client, bucket, prefix string, maxKeys int32) {
	// ContinuationToken tells S3 where the next page should continue
	// nil means: start at the beginning
	var continuationToken *string

	pageNumber := 0
	totalObjects := 0

	// Keep requesting pages until S3 tells us there are no more pages
	for {
		pageNumber++

		// Build the request for this page
		input := &s3.ListObjectsV2Input{
			// Required bucket name
			Bucket: aws.String(bucket),

			// Optional prefix filter, similar to limiting results to a folder path
			Prefix: aws.String(prefix),

			// If nil, S3 starts from the beginning. Otherwise it continues from the next page marker
			ContinuationToken: continuationToken,

			// Max number of objects returned in this single API call
			MaxKeys: aws.Int32(maxKeys),
		}

		// Call the S3 ListObjectsV2 API
		resp, err := client.ListObjectsV2(ctx, input)
		if err != nil {
			log.Fatalf("list objects failed on page %d: %v", pageNumber, err)
		}

		// Show per-page information so you can understand how pagination behaves
		fmt.Printf("Page %d\n", pageNumber)
		fmt.Printf("  KeyCount: %d\n", aws.ToInt32(resp.KeyCount))
		fmt.Printf("  MaxKeys: %d\n", maxKeys)
		fmt.Printf("  IsTruncated: %v\n", aws.ToBool(resp.IsTruncated))

		// Process the objects returned in this page
		for _, obj := range resp.Contents {
			totalObjects++
			fmt.Printf("%s\t%d\n", aws.ToString(obj.Key), obj.Size)
		}

		// If IsTruncated is false, there are no more pages and we stop
		if !aws.ToBool(resp.IsTruncated) {
			break
		}

		// Otherwise, use the token returned by S3 to request the next page
		continuationToken = resp.NextContinuationToken
	}

	fmt.Printf("\nDone. Total objects listed: %d\n", totalObjects)
}