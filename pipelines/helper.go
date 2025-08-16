package pipelines

import (
	"context"
	"fmt"
	"log"
	"sync"
)

type PipelineErrorCode string

const (
	PipelineErrorProcessing  PipelineErrorCode = "pipeline_processing"
	PipelineErrorSourceEmpty PipelineErrorCode = "pipeline_source_empty"
)

type PipelineError struct {
	Code    PipelineErrorCode
	Message string
}

func (e *PipelineError) Error() string {
	return fmt.Sprintf("Error Code %s: %s", e.Code, e.Message)
}

type ParallelConfig struct {
	Units       int
	Total       int64
	BatchSize   int64
	StartOffset int64
}

func ParallelRead[T any](
	ctx *context.Context,
	config *ParallelConfig,
	fn func(ctx *context.Context, job int, size int64, skip int64) T,
) <-chan T {

	units := config.Units
	total := config.Total
	batchSize := config.BatchSize
	startOffset := config.StartOffset

	if units <= 0 {
		units = 1
	}
	if batchSize <= 0 {
		batchSize = 1
	}

	// Calculate chunk size
	chunks := units
	chunkSize := max(1, int64((float64(total / int64(chunks)))))
	chunkSizeLeft := total % int64(chunks)
	if chunkSizeLeft > 0 {
		chunks++
	}

	// Calculate number of batches per chunk
	batchSize = min(batchSize, chunkSize)
	chunkBatches := int64(chunkSize / batchSize)
	if chunkSize%batchSize > 0 {
		chunkBatches++
	}

	// Calculate total number of batches
	batches := int64(total / batchSize)
	if total%batchSize > 0 {
		batches++
	}

	// Create parallel job channels
	jobs := make(chan int, chunks)
	results := make(chan T, chunks)
	defer close(jobs)

	// Create parallel workers
	go func() {
		var wg sync.WaitGroup

		for range chunks {
			wg.Add(1)
			go func(jobs <-chan int, results chan<- T) {
				defer wg.Done()

				job := <-jobs

				// Load chunk batches
				for j := range chunkBatches {

					chunkBatchSize := batchSize

					// Last chunk & Last batch - process leftovers
					if job == chunks-1 && j == chunkBatches-1 && chunkSizeLeft > 0 {
						chunkBatchSize = chunkSizeLeft
					}

					skip := int64(startOffset + (j * (chunkBatchSize)) + (int64(job) * chunkSize))
					results <- fn(ctx, job, chunkBatchSize, skip)
				}

			}(jobs, results)
		}

		wg.Wait()
		log.Printf("Parallel Read (Completed)\n")
		defer close(results)
	}()

	// Send jobs to workers
	log.Printf("Parallel Read (Started): Offset: %d, Total: %d, Chunks: %d, Chunk Size: %d, Batch Size: %d\n", startOffset, total, chunks, chunkSize, batchSize)
	for i := range chunks {
		jobs <- i
	}

	return results
}

func ParallelWrite[T any](
	ctx *context.Context,
	config *ParallelConfig,
	source <-chan T,
	fn func(ctx *context.Context, data T) (int64, error),
) (int64, error) {

	type WriteResult struct {
		count int64
		err   error
	}

	units := config.Units
	total := config.Total

	if units <= 0 {
		units = 1
	}

	// Calculate chunk size
	chunks := units
	if total%int64(chunks) > 0 {
		chunks++
	}

	// Create parallel job channels
	jobs := make(chan T, chunks)
	results := make(chan WriteResult, chunks)
	defer close(jobs)

	// Create parallel workers
	go func() {
		var wg sync.WaitGroup

		for i := range chunks {
			wg.Add(1)

			go func(id int, jobs <-chan T, results chan<- WriteResult) {
				defer wg.Done()

				job := <-jobs

				count, err := fn(ctx, job)
				if err != nil {
					log.Printf("Parallel Write (%d): Error: %v", id, err)
					results <- WriteResult{
						count: count,
						err:   err,
					}
				} else {
					results <- WriteResult{
						count: count,
						err:   nil,
					}
				}
			}(i, jobs, results)
		}

		wg.Wait()
		log.Printf("Parallel Write (Completed)\n")
		defer close(results)
	}()

	// Send jobs to workers
	log.Printf("Parallel Write (Started): Chunks: %d\n", chunks)
	for data := range source {
		jobs <- data
	}

	// Process result
	var count int64
	for result := range results {
		if result.err != nil {
			return count, result.err
		}
		count += result.count
	}
	log.Printf("Parallel Write (Result): %d\n", count)

	return count, nil
}
