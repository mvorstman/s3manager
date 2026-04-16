package engine

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	types "github.com/aws/aws-sdk-go-v2/service/s3/types"
)

const MaxDeleteBatchSize = 1000

type DeleteResult struct {
	Bucket      string
	Prefix      string
	DryRun      bool
	ListedPages int
	Queued      int64
	Deleted     int64
	Failed      int64
	BatchCalls  int64
	Duration    time.Duration

	TotalBatches      int64
	TotalBatchObjects int64
	AvgBatchSize      float64

	EnqueueWaitTotal time.Duration
	EnqueueWaitMax   time.Duration
	EnqueueWaitAvg   time.Duration

	DeleteLatencyTotal time.Duration
	DeleteLatencyMin   time.Duration
	DeleteLatencyMax   time.Duration
	DeleteLatencyAvg   time.Duration

	DeletesPerSecond float64
}

func updateAtomicMax(target *int64, value int64) {
	for {
		current := atomic.LoadInt64(target)
		if value <= current {
			return
		}
		if atomic.CompareAndSwapInt64(target, current, value) {
			return
		}
	}
}

func updateAtomicMin(target *int64, value int64) {
	for {
		current := atomic.LoadInt64(target)
		if current != 0 && value >= current {
			return
		}
		if atomic.CompareAndSwapInt64(target, current, value) {
			return
		}
	}
}

func DeletePrefix(
	ctx context.Context,
	client *awss3.Client,
	bucket, prefix string,
	maxKeys int32,
	dryRun bool,
	workers int,
	verbose bool,
	allowEmptyPrefix bool,
	batchSize int,
) (DeleteResult, error) {
	if prefix == "" && !allowEmptyPrefix {
		return DeleteResult{}, fmt.Errorf("for delete, --prefix is required unless --allow-empty-prefix-delete=true is set")
	}
	if workers < 1 {
		workers = 1
	}
	if maxKeys < 1 {
		maxKeys = 1000
	}
	if batchSize < 1 {
		batchSize = MaxDeleteBatchSize
	}
	if batchSize > MaxDeleteBatchSize {
		batchSize = MaxDeleteBatchSize
	}

	start := time.Now()
	result := DeleteResult{
		Bucket: bucket,
		Prefix: prefix,
		DryRun: dryRun,
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var queued int64
	var deleted int64
	var failed int64
	var batchCalls int64

	var totalBatches int64
	var totalBatchObjects int64

	var enqueueWaitTotalNs int64
	var enqueueWaitMaxNs int64

	var deleteLatencyTotalNs int64
	var deleteLatencyMinNs int64
	var deleteLatencyMaxNs int64

	if dryRun {
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
				return result, fmt.Errorf("list objects for dry-run failed on page %d: %w", result.ListedPages, err)
			}

			for _, obj := range resp.Contents {
				atomic.AddInt64(&queued, 1)
				if verbose {
					key := strings.TrimRight(aws.ToString(obj.Key), "\x00")
					fmt.Printf("DRY-RUN would delete %s\t%d\n", key, aws.ToInt64(obj.Size))
				}
			}

			if !aws.ToBool(resp.IsTruncated) {
				break
			}
			continuationToken = resp.NextContinuationToken
		}

		result.Queued = atomic.LoadInt64(&queued)
		result.Duration = time.Since(start)
		if result.Duration > 0 {
			result.DeletesPerSecond = float64(result.Queued) / result.Duration.Seconds()
		}
		return result, nil
	}

	jobs := make(chan []types.ObjectIdentifier, workers*2)
	errCh := make(chan error, workers+1)

	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			for batch := range jobs {
				if ctx.Err() != nil {
					return
				}

				deleteStart := time.Now()
				resp, err := client.DeleteObjects(ctx, &awss3.DeleteObjectsInput{
					Bucket: aws.String(bucket),
					Delete: &types.Delete{
						Objects: batch,
						Quiet:   aws.Bool(!verbose),
					},
				})
				deleteLatency := time.Since(deleteStart)

				atomic.AddInt64(&batchCalls, 1)
				atomic.AddInt64(&deleteLatencyTotalNs, deleteLatency.Nanoseconds())
				updateAtomicMin(&deleteLatencyMinNs, deleteLatency.Nanoseconds())
				updateAtomicMax(&deleteLatencyMaxNs, deleteLatency.Nanoseconds())

				if err != nil {
					select {
					case errCh <- fmt.Errorf("delete batch failed in worker %d: %w", workerID, err):
					default:
					}
					cancel()
					return
				}

				deletedCount := int64(len(resp.Deleted))
				failedCount := int64(len(resp.Errors))
				if deletedCount == 0 && failedCount == 0 {
					deletedCount = int64(len(batch))
				}

				atomic.AddInt64(&deleted, deletedCount)
				atomic.AddInt64(&failed, failedCount)

				if verbose {
					for _, obj := range resp.Deleted {
						fmt.Printf("Deleted %s\n", aws.ToString(obj.Key))
					}
					for _, delErr := range resp.Errors {
						fmt.Printf("Delete failed for %s: %s\n", aws.ToString(delErr.Key), aws.ToString(delErr.Message))
					}
				}
			}
		}(i + 1)
	}

	var continuationToken *string
	batch := make([]types.ObjectIdentifier, 0, batchSize)

	flushBatch := func() error {
		if len(batch) == 0 {
			return nil
		}

		out := make([]types.ObjectIdentifier, len(batch))
		copy(out, batch)

		enqueueStart := time.Now()

		select {
		case jobs <- out:
			enqueueWait := time.Since(enqueueStart)

			atomic.AddInt64(&totalBatches, 1)
			atomic.AddInt64(&totalBatchObjects, int64(len(out)))
			atomic.AddInt64(&enqueueWaitTotalNs, enqueueWait.Nanoseconds())
			updateAtomicMax(&enqueueWaitMaxNs, enqueueWait.Nanoseconds())

			batch = batch[:0]
			return nil

		case err := <-errCh:
			return err

		case <-ctx.Done():
			return ctx.Err()
		}
	}

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
			return result, fmt.Errorf("list objects for delete failed on page %d: %w", result.ListedPages, err)
		}

		for _, obj := range resp.Contents {
			atomic.AddInt64(&queued, 1)

			if verbose {
				key := strings.TrimRight(aws.ToString(obj.Key), "\x00")
				fmt.Printf("Queue delete %s\t%d\n", key, aws.ToInt64(obj.Size))
			}

			batch = append(batch, types.ObjectIdentifier{Key: obj.Key})
			if len(batch) == batchSize {
				if err := flushBatch(); err != nil {
					close(jobs)
					wg.Wait()
					return result, err
				}
			}
		}

		if !aws.ToBool(resp.IsTruncated) {
			break
		}
		continuationToken = resp.NextContinuationToken
	}

	if err := flushBatch(); err != nil {
		close(jobs)
		wg.Wait()
		return result, err
	}

	close(jobs)
	wg.Wait()

	select {
	case err := <-errCh:
		return result, err
	default:
	}

	result.Queued = atomic.LoadInt64(&queued)
	result.Deleted = atomic.LoadInt64(&deleted)
	result.Failed = atomic.LoadInt64(&failed)
	result.BatchCalls = atomic.LoadInt64(&batchCalls)
	result.Duration = time.Since(start)

	result.TotalBatches = atomic.LoadInt64(&totalBatches)
	result.TotalBatchObjects = atomic.LoadInt64(&totalBatchObjects)

	if result.TotalBatches > 0 {
		result.AvgBatchSize = float64(result.TotalBatchObjects) / float64(result.TotalBatches)
		result.EnqueueWaitAvg = time.Duration(atomic.LoadInt64(&enqueueWaitTotalNs) / result.TotalBatches)
	}
	result.EnqueueWaitTotal = time.Duration(atomic.LoadInt64(&enqueueWaitTotalNs))
	result.EnqueueWaitMax = time.Duration(atomic.LoadInt64(&enqueueWaitMaxNs))

	if result.BatchCalls > 0 {
		result.DeleteLatencyAvg = time.Duration(atomic.LoadInt64(&deleteLatencyTotalNs) / result.BatchCalls)
	}
	result.DeleteLatencyTotal = time.Duration(atomic.LoadInt64(&deleteLatencyTotalNs))
	result.DeleteLatencyMin = time.Duration(atomic.LoadInt64(&deleteLatencyMinNs))
	result.DeleteLatencyMax = time.Duration(atomic.LoadInt64(&deleteLatencyMaxNs))

	if result.Duration > 0 {
		result.DeletesPerSecond = float64(result.Deleted) / result.Duration.Seconds()
	}

	return result, nil
}

func PrintDeleteResult(result DeleteResult) {
	fmt.Println("Delete summary")
	fmt.Printf("  Bucket: %s\n", result.Bucket)
	fmt.Printf("  Prefix: %s\n", result.Prefix)
	fmt.Printf("  Dry-run: %v\n", result.DryRun)
	fmt.Printf("  Listed pages: %d\n", result.ListedPages)
	fmt.Printf("  Queued: %d\n", result.Queued)

	if result.DryRun {
		fmt.Printf("  Would delete: %d\n", result.Queued)
		fmt.Printf("  Deletes/sec: %.2f\n", result.DeletesPerSecond)
	} else {
		fmt.Printf("  Batch calls: %d\n", result.BatchCalls)
		fmt.Printf("  Total batches: %d\n", result.TotalBatches)
		fmt.Printf("  Avg batch size: %.2f\n", result.AvgBatchSize)
		fmt.Printf("  Deleted: %d\n", result.Deleted)
		fmt.Printf("  Failed: %d\n", result.Failed)
		fmt.Printf(
			"  Delete latency (avg/min/max): %s / %s / %s\n",
			result.DeleteLatencyAvg.Round(time.Millisecond),
			result.DeleteLatencyMin.Round(time.Millisecond),
			result.DeleteLatencyMax.Round(time.Millisecond),
		)
		fmt.Printf(
			"  Enqueue wait (avg/max): %s / %s\n",
			result.EnqueueWaitAvg.Round(time.Millisecond),
			result.EnqueueWaitMax.Round(time.Millisecond),
		)
		fmt.Printf("  Deletes/sec: %.2f\n", result.DeletesPerSecond)
	}

	fmt.Printf("  Duration: %s\n", result.Duration.Round(time.Millisecond))
}
