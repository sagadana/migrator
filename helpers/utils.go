package helpers

import (
	"context"
	"crypto/rand"
	"encoding/csv"
	"encoding/hex"
	"fmt"
	"log"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
)

type ParallelBatchConfig struct {
	// Number of parallel workers
	Units uint
	// Total items
	Total uint64
	// Number of items to process per batch
	BatchSize uint64
	// What position to start processing from
	StartOffset uint64
}

type ChunkJob struct {
	ID     uint64
	Offset uint64
	Size   uint64
}

// Run function in parallel workers
func Parallel(units uint, fn func(worker uint)) *sync.WaitGroup {
	// Normalize Units
	if units <= 0 {
		units = 1
	}

	wg := new(sync.WaitGroup)

	// Spawn workers
	for i := uint(0); i < units; i++ {
		wg.Add(1)
		go func(worker uint) {
			defer wg.Done()
			fn(worker)
		}(i)
	}

	return wg
}

// Process batch items in parallel
func ParallelBatch(
	ctx *context.Context,
	config *ParallelBatchConfig,
	fn func(ctx *context.Context, job, size, offset uint64),
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

	// Spawn workers
	wg := Parallel(units, func(_ uint) {
		for job := range jobs {
			fn(ctx, job.ID, job.Size, job.Offset)
		}
	})

	// Enqueue jobs
	for i := uint64(0); i < batches; i++ {
		size := batchSize
		if i == batches-1 && remainder > 0 {
			size = remainder
		}
		offset := start + (batchSize * i)
		jobs <- ChunkJob{
			ID:     i,
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

// Read streams of a CSV File
func StreamReadCSV(path string, batchSize uint64) (<-chan []map[string]any, error) {
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

// Save stream of contents to CSV file
func StreamWriteCSV(path string, headers []string, input <-chan []map[string]any) error {
	// Create or truncate the file
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Write headers
	if err := writer.Write(headers); err != nil {
		return err
	}

	// Write data
	for records := range input {
		for _, record := range records {
			row := make([]string, len(headers))
			for i, header := range headers {
				if val, ok := record[header]; ok {
					row[i] = fmt.Sprint(val)
				}
			}
			if err := writer.Write(row); err != nil {
				return err
			}
		}
		writer.Flush()
	}

	return nil
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

// Generates a random string for the given length
func RandomString(length int) string {
	bytes := make([]byte, length)
	_, _ = rand.Read(bytes)
	return hex.EncodeToString(bytes)[:length]
}

func CreateTextLogger(level slog.Level) *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: level,
	}))
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

func Flatten(prefix string, src any, dest map[string]any) {
	if len(prefix) > 0 && !strings.HasSuffix(prefix, ".") {
		prefix += "."
	}
	switch parent := src.(type) {
	case map[string]any:
		for k, child := range parent {
			switch gChild := child.(type) {
			case map[string]any, []any:
				Flatten(fmt.Sprintf("%s%s", prefix, k), gChild, dest)
			default:
				dest[fmt.Sprintf("%s%s", prefix, k)] = gChild
			}
		}
	case []any:
		for i, child := range parent {
			switch gChild := child.(type) {
			case map[string]any, []any:
				Flatten(fmt.Sprintf("%s%d", prefix, i), gChild, dest)
			default:
				dest[fmt.Sprintf("%s%d", prefix, i)] = gChild
			}
		}
	default:
		dest[prefix] = parent
	}
}

func Slice[T any](items []T, offset, limit uint64) (sliced []T, start, end uint64) {
	total := uint64(len(items))

	// 1. If offset beyond available entries, or empty items, return empty result
	if offset >= total || total == 0 {
		return sliced, 0, 0
	}

	// 2. Compute start index
	start = offset

	// 3. Compute end index (exclusive)
	if limit == 0 {
		// no cap: go until the end
		end = total
	} else {
		end = min(start+limit, total)
	}

	// 4. Slice out the window
	sliced = items[start:end]
	if len(sliced) == 0 {
		return sliced, 0, 0
	}

	return sliced, start, end
}
