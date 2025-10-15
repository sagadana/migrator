package helpers

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

type TestSchema = struct {
	ID       string `parquet:"name=id, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Name     string `parquet:"name=name, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Age      int32  `parquet:"name=age, type=INT32"`
	IsActive bool   `parquet:"name=is_active, type=BOOLEAN"`
}

func TestStreamReadParquet(t *testing.T) {
	t.Parallel()

	schema := map[string]reflect.Type{
		"id":        reflect.TypeOf(""),
		"name":      reflect.TypeOf(""),
		"age":       reflect.TypeOf(int32(0)),
		"is_active": reflect.TypeOf(true),
	}
	tests := []struct {
		name      string
		schema    map[string]reflect.Type
		data      [][]map[string]any
		batchSize uint64
		wantBatch [][]map[string]any // expected batches from the stream
	}{
		{
			name:   "single batch",
			schema: schema,
			data: [][]map[string]any{
				{
					{"id": "1", "name": "Alice", "age": 30, "is_active": true},
					{"id": "2", "name": "Bob", "age": 25, "is_active": false},
				},
			},
			batchSize: 10,
			wantBatch: [][]map[string]any{
				{
					{"id": "1", "name": "Alice", "age": 30, "is_active": true},
					{"id": "2", "name": "Bob", "age": 25, "is_active": false},
				},
			},
		},
		{
			name:   "multiple batches",
			schema: schema,
			data: [][]map[string]any{
				{
					{"id": "1", "name": "Alice", "age": 20, "is_active": true},
					{"id": "2", "name": "Bob", "age": 25, "is_active": false},
					{"id": "3", "name": "Charlie", "age": 30, "is_active": true},
				},
			},
			batchSize: 2,
			wantBatch: [][]map[string]any{
				{
					{"id": "1", "name": "Alice", "age": 20, "is_active": true},
					{"id": "2", "name": "Bob", "age": 25, "is_active": false},
				},
				{
					{"id": "3", "name": "Charlie", "age": 30, "is_active": true},
				},
			},
		},
		{
			name:   "empty file",
			schema: schema,
			data: [][]map[string]any{
				{},
			},
			batchSize: 10,
			wantBatch: [][]map[string]any{},
		},
		{
			name:   "batch size 1",
			schema: schema,
			data: [][]map[string]any{
				{
					{"id": "10", "name": "User1", "age": 35, "is_active": true},
					{"id": "20", "name": "User2", "age": 40, "is_active": false},
				},
			},
			batchSize: 1,
			wantBatch: [][]map[string]any{
				{{"id": "10", "name": "User1", "age": 35, "is_active": true}},
				{{"id": "20", "name": "User2", "age": 40, "is_active": false}},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Create a temp Parquet file
			tmpDir := t.TempDir()
			path := filepath.Join(tmpDir, "test.parquet")

			// Write test data to Parquet file
			if len(tt.data) > 0 && len(tt.data[0]) > 0 {
				// Create input channel
				input := make(chan []map[string]any)
				go func() {
					defer close(input)
					for _, batch := range tt.data {
						input <- batch
					}
				}()

				// Write to file using TestSchema
				if err := StreamWriteParquet(path, tt.schema, input); err != nil {
					t.Fatalf("⛔️ failed to write parquet file: %v", err)
				}
			} else {
				// Create empty file
				f, err := os.Create(path)
				if err != nil {
					t.Fatalf("⛔️ failed to create empty file: %v", err)
				}
				f.Close()
			}

			// Read back using StreamReadParquet
			stream, err := StreamReadParquet(path, tt.batchSize)
			if err != nil {
				t.Fatalf("⛔️ failed to open parquet stream: %v", err)
			}

			// Collect all batches
			var gotBatches [][]map[string]any
			for batch := range stream {
				gotBatches = append(gotBatches, batch)
			}

			// Verify number of batches
			if len(gotBatches) != len(tt.wantBatch) {
				t.Errorf("❌ got %d batches, want %d", len(gotBatches), len(tt.wantBatch))
				return
			}

			// Verify each batch
			// Note: We don't do deep equal here because parquet may return values
			// with different types or representations than what we put in
			for i, gotBatch := range gotBatches {
				if fmt.Sprintf("%#v", gotBatch) != fmt.Sprintf("%#v", tt.wantBatch[i]) {
					t.Errorf("❌ batch %d:\ngot:  %#v\nwant: %#v", i, gotBatch, tt.wantBatch[i])
				}
			}
		})
	}
}

func TestStreamReadParquet_InvalidFile(t *testing.T) {
	t.Parallel()

	// Test with non-existent file
	_, err := StreamReadParquet("/path/that/does/not/exist.parquet", 10)
	if err == nil {
		t.Fatal("⛔️ expected error for non-existent file, got nil")
	}
}

func TestStreamWriteParquet(t *testing.T) {
	t.Parallel()

	schema := map[string]reflect.Type{
		"id":        reflect.TypeOf(""),
		"name":      reflect.TypeOf(""),
		"age":       reflect.TypeOf(int32(0)),
		"is_active": reflect.TypeOf(true),
	}
	tests := []struct {
		name        string
		schema      map[string]reflect.Type
		data        [][]map[string]any
		wantErr     bool
		expectedLen int // number of rows expected
	}{
		{
			name:   "simple write",
			schema: schema,
			data: [][]map[string]any{
				{
					{"id": "1", "name": "Alice", "age": 30, "is_active": true},
					{"id": "2", "name": "Bob", "age": 25, "is_active": false},
				},
			},
			wantErr:     false,
			expectedLen: 2,
		},
		{
			name:   "multiple batches",
			schema: schema,
			data: [][]map[string]any{
				{
					{"id": "10", "name": "User1", "age": 20, "is_active": true},
					{"id": "20", "name": "User2", "age": 30, "is_active": false},
				},
				{
					{"id": "30", "name": "User3", "age": 40, "is_active": true},
				},
			},
			wantErr:     false,
			expectedLen: 3,
		},
		{
			name:        "empty data",
			schema:      map[string]reflect.Type{},
			data:        [][]map[string]any{},
			wantErr:     false,
			expectedLen: 0,
		},
		{
			name:   "missing fields",
			schema: schema,
			data: [][]map[string]any{
				{
					{"id": "1", "name": "Alice"},              // missing 'age' and 'is_active'
					{"id": "2", "age": 25, "is_active": true}, // missing 'name'
				},
			},
			wantErr:     false,
			expectedLen: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			tmpDir := t.TempDir()
			path := filepath.Join(tmpDir, "output.parquet")

			// Setup error channel
			errCh := make(chan error, 1)
			input := make(chan []map[string]any)

			// Write asynchronously
			go func() {
				err := StreamWriteParquet(path, tt.schema, input)
				errCh <- err
			}()

			// Send data batches
			go func() {
				defer close(input)
				for _, batch := range tt.data {
					input <- batch
				}
			}()

			// Wait for completion
			err := <-errCh
			if tt.wantErr {
				if err == nil {
					t.Fatalf("⛔️ expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("⛔️ unexpected error: %v", err)
			}

			// Verify file was created
			info, err := os.Stat(path)
			if err != nil {
				t.Fatalf("⛔️ failed to stat output file: %v", err)
			}

			if tt.expectedLen == 0 && info.Size() == 0 {
				// Empty file is acceptable for empty data
				return
			}

			// Read back and verify using StreamReadParquet
			stream, err := StreamReadParquet(path, 100)
			if err != nil {
				t.Fatalf("⛔️ failed to read parquet file: %v", err)
			}

			rowCount := 0
			for batch := range stream {
				rowCount += len(batch)
			}

			if rowCount != tt.expectedLen {
				t.Errorf("❌ got %d rows, want %d", rowCount, tt.expectedLen)
			}
		})
	}
}

func TestStreamWriteParquet_InvalidPath(t *testing.T) {
	t.Parallel()

	// Test with invalid file path
	input := make(chan []map[string]any)
	close(input)

	schema := map[string]reflect.Type{"id": reflect.TypeOf("")}
	err := StreamWriteParquet("/invalid/path/that/does/not/exist.parquet", schema, input)
	if err == nil {
		t.Fatal("⛔️ expected error for invalid path, got nil")
	}
}

func TestStreamParquetRoundTrip(t *testing.T) {
	t.Parallel()

	// Test that we can write and read back the same data
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "roundtrip.parquet")

	schema := map[string]reflect.Type{
		"id":    reflect.TypeOf(""),
		"name":  reflect.TypeOf(""),
		"value": reflect.TypeOf(int32(0)),
	}
	testData := [][]map[string]any{
		{
			{"id": "1", "name": "test1", "value": 100},
			{"id": "2", "name": "test2", "value": 200},
			{"id": "3", "name": "test3", "value": 300},
		},
	}

	// Write
	input := make(chan []map[string]any)
	go func() {
		defer close(input)
		for _, batch := range testData {
			input <- batch
		}
	}()

	if err := StreamWriteParquet(path, schema, input); err != nil {
		t.Fatalf("⛔️ failed to write: %v", err)
	}

	// Read back
	stream, err := StreamReadParquet(path, 10)
	if err != nil {
		t.Fatalf("⛔️ failed to read: %v", err)
	}

	var readData []map[string]any
	for batch := range stream {
		readData = append(readData, batch...)
	}

	// Verify
	if len(readData) != len(testData[0]) {
		t.Errorf("❌ got %d rows, want %d", len(readData), len(testData[0]))
	}

	// Note: We don't do deep equal here because parquet may return values
	// with different types or representations than what we put in
	for i, row := range readData {
		expected := testData[0][i]
		if fmt.Sprintf("%#v", row) != fmt.Sprintf("%#v", expected) {
			t.Errorf("❌ row %d:\ngot:  %#v\nwant: %#v", i, row, expected)
		}
	}
}
