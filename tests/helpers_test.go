package tests

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/sagadana/migrator/helpers"
)

// -------------------
// UTILS
// -------------------

// jobCall records one invocation of the user-supplied fn.
type jobCall struct {
	ID     int
	Size   uint64
	Offset uint64
	CtxPtr *context.Context
}

// sortJobs sorts a slice of jobCall by ID for deterministic comparisons.
func sortJobs(calls []jobCall) {
	sort.Slice(calls, func(i, j int) bool {
		return calls[i].ID < calls[j].ID
	})
}

func TestParallelBatch(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name     string
		config   helpers.ParallelConfig
		expected []jobCall
	}{
		{
			name: "Basic single worker, partial last batch",
			config: helpers.ParallelConfig{
				Units:       1,
				Total:       5,
				BatchSize:   2,
				StartOffset: 10,
			},
			expected: []jobCall{
				{ID: 0, Offset: 10, Size: 2},
				{ID: 1, Offset: 12, Size: 2},
				{ID: 2, Offset: 14, Size: 1},
			},
		},
		{
			name: "BatchSize > Total (one big batch)",
			config: helpers.ParallelConfig{
				Units:       3,
				Total:       4,
				BatchSize:   10,
				StartOffset: 0,
			},
			expected: []jobCall{
				{ID: 0, Offset: 0, Size: 4},
			},
		},
		{
			name: "Zero BatchSize fallback to 1",
			config: helpers.ParallelConfig{
				Units:       2,
				Total:       3,
				BatchSize:   0,
				StartOffset: 5,
			},
			expected: []jobCall{
				{ID: 0, Offset: 5, Size: 1},
				{ID: 1, Offset: 6, Size: 1},
				{ID: 2, Offset: 7, Size: 1},
			},
		},
		{
			name: "Zero Units fallback to 1",
			config: helpers.ParallelConfig{
				Units:       0,
				Total:       3,
				BatchSize:   2,
				StartOffset: 2,
			},
			expected: []jobCall{
				{ID: 0, Offset: 2, Size: 2},
				{ID: 1, Offset: 4, Size: 1},
			},
		},
		{
			name: "Multiple workers, even split with remainder",
			config: helpers.ParallelConfig{
				Units:       3,
				Total:       7,
				BatchSize:   2,
				StartOffset: 0,
			},
			expected: []jobCall{
				{ID: 0, Offset: 0, Size: 2},
				{ID: 1, Offset: 2, Size: 2},
				{ID: 2, Offset: 4, Size: 2},
				{ID: 3, Offset: 6, Size: 1},
			},
		},
		{
			name:   "Total zero yields no jobs",
			config: helpers.ParallelConfig{Units: 5, Total: 0, BatchSize: 3},
			// expected empty slice
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var (
				mu    sync.Mutex
				calls []jobCall
			)

			// Wrap the fn to capture calls
			helpers.ParallelBatch(&ctx, &tt.config, func(c *context.Context, id int, size, offset uint64) {
				mu.Lock()
				defer mu.Unlock()
				calls = append(calls, jobCall{
					ID:     id,
					Size:   size,
					Offset: offset,
					CtxPtr: c,
				})
			})

			// Verify call count
			if len(calls) != len(tt.expected) {
				t.Fatalf("⛔️ got %d calls; want %d", len(calls), len(tt.expected))
			}

			// Sort both slices by ID for comparison
			sortJobs(calls)
			sortJobs(tt.expected)

			// Compare each call
			for i, exp := range tt.expected {
				got := calls[i]
				if got.ID != exp.ID || got.Size != exp.Size || got.Offset != exp.Offset {
					t.Errorf("❌ call %d: got (ID=%d, Offset=%d, Size=%d), want (ID=%d, Offset=%d, Size=%d)",
						i, got.ID, got.Offset, got.Size, exp.ID, exp.Offset, exp.Size)
				}
				// Check that the context pointer is identical
				if got.CtxPtr != &ctx {
					t.Errorf("❌ call %d: expected same context pointer %p, got %p", i, &ctx, got.CtxPtr)
				}
			}
		})
	}
}

// create temp file with content
func writeTempFile(t *testing.T, content string) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "test.csv")
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("⛔️ failed to write temp file: %v", err)
	}
	return path
}

func TestStreamCSV(t *testing.T) {
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
			var path string
			if tt.wantErr {
				path = "no_such_file.csv"
			} else {
				path = writeTempFile(t, tt.content)
			}

			ch, err := helpers.StreamCSV(path, tt.batchSize)
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

func TestExtractNumber(t *testing.T) {
	tests := []struct {
		input string
		want  int
	}{
		{"abc123def", 123},
		{"no numbers here", 0},
		{"1start2", 1},
		{"middle 456 text 789", 456},
		{"", 0},
	}
	for _, tt := range tests {
		got := helpers.ExtractNumber(tt.input)
		if got != tt.want {
			t.Errorf("ExtractNumber(%q) = %d; want %d", tt.input, got, tt.want)
		}
	}
}

func TestNumberAwareSort(t *testing.T) {
	data := []string{"file10.txt", "file2.txt", "file1.txt", "file2a.txt", "file2b.txt"}
	helpers.NumberAwareSort(data)
	want := []string{"file1.txt", "file2.txt", "file2a.txt", "file2b.txt", "file10.txt"}
	for i := range want {
		if data[i] != want[i] {
			t.Errorf("NumberAwareSort result[%d] = %q; want %q", i, data[i], want[i])
		}
	}
}

func TestCalculateHash(t *testing.T) {
	data := []byte("hello world")
	got := helpers.CalculateHash(data)
	expectedArr := sha256.Sum256(data)
	want := hex.EncodeToString(expectedArr[:])
	if got != want {
		t.Errorf("CalculateHash = %q; want %q", got, want)
	}
}

func TestRandomString(t *testing.T) {
	length := 16
	s1 := helpers.RandomString(length)
	s2 := helpers.RandomString(length)
	if len(s1) != length {
		t.Errorf("RandomString length = %d; want %d", len(s1), length)
	}
	if len(s2) != length {
		t.Errorf("RandomString length = %d; want %d", len(s2), length)
	}
	if s1 == s2 {
		t.Errorf("RandomString produced identical strings %q and %q", s1, s2)
	}
	for _, r := range s1 {
		if !strings.Contains("0123456789abcdef", string(r)) {
			t.Errorf("RandomString contains invalid char %q", r)
		}
	}
}

func TestCreateTextLogger(t *testing.T) {
	logger := helpers.CreateTextLogger()
	if logger == nil {
		t.Fatal("CreateTextLogger returned nil")
	}
}

func TestGetTempBasePath(t *testing.T) {
	id := "mypath"
	origRunner := os.Getenv("RUNNER_TEMP")
	origBase := os.Getenv("TEMP_BASE_PATH")
	defer func() {
		os.Setenv("RUNNER_TEMP", origRunner)
		os.Setenv("TEMP_BASE_PATH", origBase)
	}()

	os.Unsetenv("RUNNER_TEMP")
	os.Unsetenv("TEMP_BASE_PATH")
	want := filepath.Join(os.TempDir(), id)
	got := helpers.GetTempBasePath(id)
	if got != want {
		t.Errorf("GetTempBasePath fallback = %q; want %q", got, want)
	}

	os.Unsetenv("RUNNER_TEMP")
	os.Setenv("TEMP_BASE_PATH", "/basepath")
	want = filepath.Join("/basepath", id)
	got = helpers.GetTempBasePath(id)
	if got != want {
		t.Errorf("GetTempBasePath TEMP_BASE_PATH = %q; want %q", got, want)
	}

	os.Setenv("RUNNER_TEMP", "/runner")
	os.Setenv("TEMP_BASE_PATH", "/basepath")
	want = filepath.Join("/runner", id)
	got = helpers.GetTempBasePath(id)
	if got != want {
		t.Errorf("GetTempBasePath RUNNER_TEMP = %q; want %q", got, want)
	}
}
