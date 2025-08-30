package helpers

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/csv"
	"encoding/hex"
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

type ChunkJob struct {
	ID     int
	Offset int64
	Size   int64
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
	chunks := int64(units)
	chunkSize := max(1, int64((float64(total / chunks))))
	if total%chunks > 0 {
		chunks++
	}

	// Calculate bach size per chunk
	batchSize = min(batchSize, chunkSize)
	batches := total / batchSize
	batchSizeLeft := total % batchSize
	if batchSizeLeft > 0 {
		batches++
	}

	// Create parallel job channels
	jobs := make(chan ChunkJob, batches)
	out := make(chan T)
	wg := new(sync.WaitGroup)

	// Create parallel workers
	for i := range units {
		wg.Add(1)

		go func(wrk int) {
			defer wg.Done()

			for job := range jobs {
				out <- fn(ctx, job.ID, job.Size, job.Offset)
			}
		}(i)
	}

	// Send jobs to workers
	log.Printf(
		"Parallel Read (Started): Units: %d, Offset: %d, Total: %d, Batches: %d, Batch Size: %d, Batch Size Left: %d\n",
		units, startOffset, total, batches, batchSize, batchSizeLeft,
	)

	for i := int64(0); i < batches; i++ {
		size := batchSize
		if i == batches-1 && batchSizeLeft > 0 {
			size = batchSizeLeft
		}

		jobs <- ChunkJob{
			ID:     int(i),
			Offset: startOffset + (batchSize * i),
			Size:   size,
		}
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

// Computes a SHA256 hash for a given byte slice.
func CalculateHash(data []byte) string {
	hash := sha256.Sum256(data)
	return hex.EncodeToString(hash[:])
}

// Generates a random string for the given length
func RandomString(length int) string {
	bytes := make([]byte, length)
	_, _ = rand.Read(bytes)
	return hex.EncodeToString(bytes)[:length]
}
