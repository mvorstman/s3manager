package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	types "github.com/aws/aws-sdk-go-v2/service/s3/types"
)

type deleteBatchJob struct {
	objects []types.ObjectIdentifier
}

func deleteObjectsByPrefix(ctx context.Context, client *s3.Client, bucket, prefix string, maxKeys int32, dryRun bool, workers int, verbose bool) {
	if prefix == "" {
		log.Fatal("for delete, --prefix is required as a safety measure")
	}

	startTime := time.Now()

	fmt.Println("Delete operation starting...")
	fmt.Printf("  Bucket: %s\n", bucket)
	fmt.Printf("  Prefix: %s\n", prefix)
	fmt.Printf("  Dry-run: %v\n", dryRun)
	fmt.Printf("  MaxKeys: %d\n", maxKeys)
	fmt.Printf("  Workers: %d\n", workers)
	fmt.Printf("  Verbose: %v\n", verbose)

	var totalMatched int64
	var totalDeleted int64
	var batchesQueued int64
	var batchesCompleted int64

	progressInterval := int64(100)

	// Channel = wachtrij voor delete batches
	jobCh := make(chan deleteBatchJob, workers*2)

	// Error channel: eerste fout wint
	errCh := make(chan error, 1)

	// ---- Start delete workers ----
	var workerWG sync.WaitGroup

	if !dryRun {
		for workerID := 1; workerID <= workers; workerID++ {
			workerWG.Add(1)

			go func(id int) {
				defer workerWG.Done()

				for job := range jobCh {
					deletedCount, err := deleteBatch(ctx, client, bucket, job.objects, verbose, id)
					if err != nil {
						select {
						case errCh <- err:
						default:
						}
						return
					}

					newDeleted := atomic.AddInt64(&totalDeleted, int64(deletedCount))
					newCompleted := atomic.AddInt64(&batchesCompleted, 1)

					if !verbose {
						fmt.Printf("[progress] batches %d - deleted %d objects\n",
							newCompleted,
							newDeleted,
						)
					}
				}
			}(workerID)
		}
	}

	// ---- Producer: scan objecten en stuur batches direct naar workers ----
	var continuationToken *string
	pageNumber := 0

producerLoop:
	for {
		// Stop vroeg als een worker al een fout heeft gemeld
		select {
		case err := <-errCh:
			log.Fatalf("delete failed: %v", err)
		default:
		}

		pageNumber++

		input := &s3.ListObjectsV2Input{
			Bucket:            aws.String(bucket),
			Prefix:            aws.String(prefix),
			ContinuationToken: continuationToken,
			MaxKeys:           aws.Int32(maxKeys),
		}

		resp, err := client.ListObjectsV2(ctx, input)
		if err != nil {
			log.Fatalf("delete scan failed on page %d: %v", pageNumber, err)
		}

		if verbose {
			fmt.Printf("\nScan page %d\n", pageNumber)
			fmt.Printf("  KeyCount: %d\n", aws.ToInt32(resp.KeyCount))
			fmt.Printf("  IsTruncated: %v\n", aws.ToBool(resp.IsTruncated))
		}

		var batch []types.ObjectIdentifier

		for _, obj := range resp.Contents {
			key := aws.ToString(obj.Key)
			newMatched := atomic.AddInt64(&totalMatched, 1)

			if dryRun {
				if verbose {
					fmt.Printf("DRY-RUN would delete: %s\t%d\n", key, obj.Size)
				} else if newMatched%progressInterval == 0 {
					fmt.Printf("[scan] matched %d objects\n", newMatched)
				}
				continue
			}

			if verbose {
				fmt.Printf("Queued for delete: %s\t%d\n", key, obj.Size)
			}

			batch = append(batch, types.ObjectIdentifier{
				Key: aws.String(key),
			})

			// DeleteObjects ondersteunt maximaal 1000 keys per batch
			if len(batch) == 1000 {
				select {
				case err := <-errCh:
					log.Fatalf("delete failed: %v", err)
				case jobCh <- deleteBatchJob{objects: batch}:
					atomic.AddInt64(&batchesQueued, 1)
				}
				batch = nil
			}
		}

		// Restbatch ook versturen
		if len(batch) > 0 && !dryRun {
			select {
			case err := <-errCh:
				log.Fatalf("delete failed: %v", err)
			case jobCh <- deleteBatchJob{objects: batch}:
				atomic.AddInt64(&batchesQueued, 1)
			}
		}

		if !aws.ToBool(resp.IsTruncated) {
			break producerLoop
		}

		continuationToken = resp.NextContinuationToken
	}

	// Producer is klaar; geen nieuwe jobs meer
	close(jobCh)

	// Dry-run heeft geen workers draaien
	if dryRun {
		duration := time.Since(startTime)

		fmt.Printf("\nScan complete.\n")
		fmt.Printf("  Total matched: %d\n", totalMatched)
		fmt.Printf("  Total deleted: 0 (dry-run)\n")
		fmt.Printf("  Duration: %.2fs\n", duration.Seconds())
		return
	}

	// Wacht tot workers klaar zijn
	doneCh := make(chan struct{})
	go func() {
		workerWG.Wait()
		close(doneCh)
	}()

	select {
	case err := <-errCh:
		log.Fatalf("parallel delete failed: %v", err)
	case <-doneCh:
	}

	duration := time.Since(startTime)

	fmt.Printf("\nDelete complete.\n")
	fmt.Printf("  Total matched: %d\n", totalMatched)
	fmt.Printf("  Batches queued: %d\n", batchesQueued)
	fmt.Printf("  Batches completed: %d\n", batchesCompleted)
	fmt.Printf("  Total deleted: %d\n", totalDeleted)
	fmt.Printf("  Duration: %.2fs\n", duration.Seconds())

	if duration.Seconds() > 0 {
		fmt.Printf("  Delete rate: %.2f objects/s\n", float64(totalDeleted)/duration.Seconds())
	}
}

func deleteBatch(ctx context.Context, client *s3.Client, bucket string, batch []types.ObjectIdentifier, verbose bool, workerID int) (int, error) {
	if verbose {
		fmt.Printf("[worker %d] Deleting batch of %d objects...\n", workerID, len(batch))
	}

	resp, err := client.DeleteObjects(ctx, &s3.DeleteObjectsInput{
		Bucket: aws.String(bucket),
		Delete: &types.Delete{
			Objects: batch,
			Quiet:   aws.Bool(false),
		},
	})
	if err != nil {
		return 0, fmt.Errorf("worker %d batch delete failed: %w", workerID, err)
	}

	if verbose {
		for _, d := range resp.Deleted {
			fmt.Printf("[worker %d] Deleted: %s\n", workerID, aws.ToString(d.Key))
		}

		for _, e := range resp.Errors {
			fmt.Printf("[worker %d] Delete error: key=%s code=%s message=%s\n",
				workerID,
				aws.ToString(e.Key),
				aws.ToString(e.Code),
				aws.ToString(e.Message),
			)
		}
	}

	return len(resp.Deleted), nil
}