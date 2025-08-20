package helpers

import (
	"context"
	"encoding/csv"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
)

type ParallelConfig struct {
	Units       int
	Total       int64
	BatchSize   int64
	StartOffset int64
}

// Process batch reads in parallel
func ParallelRead[T any](
	ctx *context.Context,
	config *ParallelConfig,
	fn func(ctx *context.Context, job int, size int64, offset int64) T,
) <-chan T {

	units := config.Units
	total := config.Total
	batchSize := min(config.BatchSize, total)
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
	jobs := make(chan int)
	out := make(chan T)
	wg := new(sync.WaitGroup)

	// Create parallel workers
	for range chunks {
		wg.Add(1)

		go func() {
			defer wg.Done()

			job := <-jobs

			// Load chunk batches
			for b := range chunkBatches {

				chunkBatchSize := batchSize

				// Last chunk & Last batch - process leftovers
				if job == chunks-1 && b == chunkBatches-1 && chunkSizeLeft > 0 {
					chunkBatchSize = chunkSizeLeft
				}

				offset := int64(startOffset + (b * (chunkBatchSize)) + (int64(job) * chunkSize))
				out <- fn(ctx, job, chunkBatchSize, offset)
			}
		}()
	}

	// Send jobs to workers
	log.Printf(
		"Parallel Read (Started): Units: %d, Offset: %d, Total: %d, Chunks: %d, Chunk Size: %d, Batch Size: %d\n",
		units, startOffset, total, chunks, chunkSize, batchSize,
	)
	for i := range chunks {
		jobs <- i
	}
	close(jobs) // Close job channel - no more jobs to be submitted

	// Background waits for results to be collected
	// Close out channel after
	go func() {
		wg.Wait()
		close(out)
	}()

	return out
}

// Transform stream contents
func StreamTransform[In, Out any](input <-chan In, fn func(data In) Out) <-chan Out {
	output := make(chan Out)
	go func() {
		defer close(output)
		for data := range input {
			output <- fn(data)
		}
	}()
	return output
}

// Stream contents of a CSV File
func StreamCSV(path string, batchSize int64) (<-chan []map[string]any, error) {
	out := make(chan []map[string]any)

	if batchSize <= 0 {
		batchSize = 1
	}

	// Open the file
	file, err := os.Open(path)
	if err != nil {
		return out, err
	}

	go func() {
		defer file.Close()
		defer close(out)

		// Create a CSV reader
		reader := csv.NewReader(file)

		// Read the header row
		headers, err := reader.Read()
		if err != nil {
			log.Println("Error reading header:", err)
			return
		}

		// Batch
		records := make([]map[string]any, 0)

		// Stream the CSV content to the output stream
		for {

			row, err := reader.Read()
			if err != nil {
				if err.Error() == "EOF" {
					break // End of file
				}
				log.Println("Error reading record:", err)
				return
			}

			// Map the row to a map[string]string using the header
			record := make(map[string]any)
			for i, value := range row {
				record[headers[i]] = value
			}

			// Add to batch
			records = append(records, record)

			// Write the records to the output stream
			if len(records) >= int(batchSize) {
				out <- records
				records = make([]map[string]any, 0)
			}
		}

		// Write remaining records
		if len(records) > 0 {
			out <- records
		}
	}()

	return out, nil
}

// Custom sorting function
func NumberAwareSort(data []string) {
	sort.Slice(data, func(i, j int) bool {
		// Extract numeric parts from strings
		num1 := ExtractNumber((data)[i])
		num2 := ExtractNumber(data[j])

		// Compare numeric parts
		if num1 != num2 {
			return num1 < num2
		}

		// If numeric parts are equal, compare strings lexicographically
		return data[i] < data[j]
	})
}

// Helper function to extract the numeric part from a string
func ExtractNumber(s string) int {
	// Find the first numeric part in the string
	for _, part := range strings.FieldsFunc(s, func(r rune) bool {
		return r < '0' || r > '9'
	}) {
		if num, err := strconv.Atoi(part); err == nil {
			return num
		}
	}
	return 0 // Default to 0 if no number is found
}
