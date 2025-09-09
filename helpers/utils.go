package helpers

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/csv"
	"encoding/hex"
	"log"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
)

type ParallelConfig struct {
	// Number of parallel workers
	Units int
	// Total items
	Total uint64
	// Number of items to process per batch
	BatchSize uint64
	// What position to start processing from
	StartOffset uint64
}

type ChunkJob struct {
	ID     int
	Offset uint64
	Size   uint64
}

// Process batch items in parallel
func ParallelBatch(
	ctx *context.Context,
	config *ParallelConfig,
	fn func(ctx *context.Context, job int, size, offset uint64),
) {

	// Normalize Units
	units := config.Units
	if units <= 0 {
		units = 1
	}

	// Normalize BatchSize
	batchSize := config.BatchSize
	if batchSize == 0 {
		batchSize = 1
	}

	total := config.Total
	start := config.StartOffset

	// No work if total is zero
	if total == 0 {
		return
	}

	// Compute how many batches we need
	fullBatches := total / batchSize
	remainder := total % batchSize
	batches := fullBatches
	if remainder > 0 {
		batches++
	}

	// Buffered channel holds all jobs
	jobs := make(chan ChunkJob, batches)
	var wg sync.WaitGroup

	// Spawn workers
	for range units {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for job := range jobs {
				fn(ctx, job.ID, job.Size, job.Offset)
			}
		}()
	}

	// Enqueue jobs
	for i := uint64(0); i < batches; i++ {
		size := batchSize
		if i == batches-1 && remainder > 0 {
			size = remainder
		}
		offset := start + (batchSize * i)
		jobs <- ChunkJob{
			ID:     int(i),
			Offset: offset,
			Size:   size,
		}
	}
	close(jobs)

	// Wait for all workers to finish
	wg.Wait()
}

// Transform stream contents
func StreamTransform[In, Out any](input <-chan In, fn func(data In) Out) <-chan Out {
	output := make(chan Out)
	go func() {
		defer close(output)
		var data In // reusable var - reduce memory consumption and gc work
		for data = range input {
			output <- fn(data)
		}
	}()
	return output
}

// Stream contents of a CSV File
func StreamCSV(path string, batchSize uint64) (<-chan []map[string]any, error) {
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
		defer close(out)
		defer file.Close()

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

func CreateTextLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stdout, nil))
}

func GetTempBasePath(id string) string {
	basePath := os.Getenv("RUNNER_TEMP")
	if basePath == "" {
		basePath = os.Getenv("TEMP_BASE_PATH")
		if basePath == "" {
			basePath = os.TempDir()
		}
	}
	return filepath.Join(basePath, id)
}
