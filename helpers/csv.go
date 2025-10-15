package helpers

import (
	"encoding/csv"
	"fmt"
	"log/slog"
	"os"
)

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
			slog.Warn("error reading header", "error", err)
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
				slog.Warn("error reading record", "error", err)
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
