package helpers

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestStreamCSV(t *testing.T) {
	t.Parallel()

	// Helper to build CSV content from 2D array
	toCSV := func(rows [][]string) string {
		if len(rows) == 0 {
			return ""
		}
		var s string
		for i, row := range rows {
			for j, col := range row {
				if j > 0 {
					s += ","
				}
				s += col
			}
			if i < len(rows)-1 {
				s += "\n"
			}
		}
		return s
	}

	tests := []struct {
		name      string
		content   string
		batchSize uint64
		want      [][]map[string]any
		wantErr   bool
	}{
		{
			name: "basic batching",
			content: toCSV([][]string{
				{"col1", "col2"},
				{"a", "1"},
				{"b", "2"},
				{"c", "3"},
			}),
			batchSize: 2,
			want: [][]map[string]any{
				{
					{"col1": "a", "col2": "1"},
					{"col1": "b", "col2": "2"},
				},
				{
					{"col1": "c", "col2": "3"},
				},
			},
		},
		{
			name: "batchSize greater than total",
			content: toCSV([][]string{
				{"x", "y"},
				{"u", "10"},
				{"v", "20"},
				{"w", "30"},
			}),
			batchSize: 10,
			want: [][]map[string]any{
				{
					{"x": "u", "y": "10"},
					{"x": "v", "y": "20"},
					{"x": "w", "y": "30"},
				},
			},
		},
		{
			name: "zero batchSize fallback to 1",
			content: toCSV([][]string{
				{"h1", "h2"},
				{"a", "b"},
				{"c", "d"},
			}),
			batchSize: 0,
			want: [][]map[string]any{
				{
					{"h1": "a", "h2": "b"},
				},
				{
					{"h1": "c", "h2": "d"},
				},
			},
		},
		{
			name:      "empty file yields no batches",
			content:   toCSV([][]string{{"h1", "h2"}}),
			batchSize: 3,
			want:      [][]map[string]any{},
		},
		{
			name:    "nonexistent file returns error",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var path string
			if tt.wantErr {
				path = "no_such_file.csv"
			} else {
				path = writeTempFile(t, tt.content)
			}

			ch, err := StreamReadCSV(path, tt.batchSize)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("⛔️ expected error for path %q, got nil", path)
				}
				return
			}
			if err != nil {
				t.Fatalf("⛔️ unexpected error: %v", err)
			}

			got := make([][]map[string]any, 0)
			for batch := range ch {
				got = append(got, batch)
			}

			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("❌ got %v, want %v", got, tt.want)
			}
		})
	}
}

func TestStreamWriteCSV(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		headers     []string
		data        [][]map[string]any
		expectedCSV string
		wantErr     bool
	}{
		{
			name:    "basic single batch",
			headers: []string{"id", "name", "age"},
			data: [][]map[string]any{
				{
					{"id": "1", "name": "Alice", "age": 30},
					{"id": "2", "name": "Bob", "age": 25},
				},
			},
			expectedCSV: "id,name,age\n1,Alice,30\n2,Bob,25\n",
		},
		{
			name:    "multiple batches",
			headers: []string{"x", "y"},
			data: [][]map[string]any{
				{
					{"x": "a", "y": "1"},
				},
				{
					{"x": "b", "y": "2"},
					{"x": "c", "y": "3"},
				},
			},
			expectedCSV: "x,y\na,1\nb,2\nc,3\n",
		},
		{
			name:    "missing fields filled with empty",
			headers: []string{"col1", "col2", "col3"},
			data: [][]map[string]any{
				{
					{"col1": "val1", "col3": "val3"}, // missing col2
					{"col1": "val4", "col2": "val5", "col3": "val6"},
				},
			},
			expectedCSV: "col1,col2,col3\nval1,,val3\nval4,val5,val6\n",
		},
		{
			name:        "empty data with headers only",
			headers:     []string{"h1", "h2"},
			data:        [][]map[string]any{},
			expectedCSV: "h1,h2\n",
		},
		{
			name:    "numeric and boolean values",
			headers: []string{"id", "score", "active"},
			data: [][]map[string]any{
				{
					{"id": 1, "score": 95.5, "active": true},
					{"id": 2, "score": 88, "active": false},
				},
			},
			expectedCSV: "id,score,active\n1,95.5,true\n2,88,false\n",
		},
		{
			name:    "empty headers",
			headers: []string{},
			data: [][]map[string]any{
				{
					{"ignored": "value"},
				},
			},
			expectedCSV: "\n\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Create temp directory and file path
			dir := t.TempDir()
			path := filepath.Join(dir, "test.csv")

			// Create input channel
			input := make(chan []map[string]any)

			// Start StreamWriteCSV in goroutine
			errCh := make(chan error, 1)
			go func() {
				defer close(errCh)
				errCh <- StreamWriteCSV(path, tt.headers, input)
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

			// Read and verify the file
			content, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("⛔️ failed to read output file: %v", err)
			}

			if string(content) != tt.expectedCSV {
				t.Errorf("❌ got CSV content:\n%q\nwant:\n%q", string(content), tt.expectedCSV)
			}
		})
	}
}

func TestStreamWriteCSV_InvalidPath(t *testing.T) {
	t.Parallel()

	// Test with invalid file path
	input := make(chan []map[string]any)
	close(input)

	err := StreamWriteCSV("/invalid/path/that/does/not/exist.csv", []string{"h1"}, input)
	if err == nil {
		t.Fatal("⛔️ expected error for invalid path, got nil")
	}
}
