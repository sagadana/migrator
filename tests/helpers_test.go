package tests

import (
	"context"
	"log/slog"
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
			calls := make([]jobCall, 0)
			mu := new(sync.Mutex)

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

			ch, err := helpers.StreamReadCSV(path, tt.batchSize)
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
	t.Parallel()

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
			t.Errorf("❌ ExtractNumber(%q) = %d; want %d", tt.input, got, tt.want)
		}
	}
}

func TestNumberAwareSort(t *testing.T) {
	t.Parallel()

	data := []string{"file10.txt", "file2.txt", "file1.txt", "file2a.txt", "file2b.txt"}
	helpers.NumberAwareSort(data)
	want := []string{"file1.txt", "file2.txt", "file2a.txt", "file2b.txt", "file10.txt"}
	for i := range want {
		if data[i] != want[i] {
			t.Errorf("❌ NumberAwareSort result[%d] = %q; want %q", i, data[i], want[i])
		}
	}
}

func TestRandomString(t *testing.T) {
	t.Parallel()

	length := 16
	s1 := helpers.RandomString(length)
	s2 := helpers.RandomString(length)
	if len(s1) != length {
		t.Errorf("❌ RandomString length = %d; want %d", len(s1), length)
	}
	if len(s2) != length {
		t.Errorf("❌ RandomString length = %d; want %d", len(s2), length)
	}
	if s1 == s2 {
		t.Errorf("❌ RandomString produced identical strings %q and %q", s1, s2)
	}
	for _, r := range s1 {
		if !strings.Contains("0123456789abcdef", string(r)) {
			t.Errorf("❌ RandomString contains invalid char %q", r)
		}
	}
}

func TestCreateTextLogger(t *testing.T) {
	t.Parallel()

	logger := helpers.CreateTextLogger(slog.LevelDebug)
	if logger == nil {
		t.Fatal("CreateTextLogger returned nil")
	}
	if logger.Handler() == nil {
		t.Fatal("CreateTextLogger returned logger with nil handler")
	}

	ctx := context.Background()
	// Test logging at different levels
	if logger.Enabled(ctx, slog.LevelInfo) != true {
		t.Error("Expected Info level to be enabled")
	}
	if logger.Enabled(ctx, slog.LevelDebug) != true {
		t.Error("Expected Debug level to be enabled")
	}
	if logger.Enabled(ctx, slog.LevelWarn) != true {
		t.Error("Expected Warn level to be enabled")
	}
	if logger.Enabled(ctx, slog.LevelError) != true {
		t.Error("Expected Error level to be enabled")
	}

	// Create logger with higher level

	logger = helpers.CreateTextLogger(slog.LevelError)
	if logger.Enabled(ctx, slog.LevelInfo) != false {
		t.Error("Expected Info level to be disabled")
	}
	if logger.Enabled(ctx, slog.LevelDebug) != false {
		t.Error("Expected Debug level to be disabled")
	}
	if logger.Enabled(ctx, slog.LevelWarn) != false {
		t.Error("Expected Warn level to be disabled")
	}
	if logger.Enabled(ctx, slog.LevelError) != true {
		t.Error("Expected Error level to be enabled")
	}
}

func TestGetTempBasePath(t *testing.T) {
	t.Parallel()

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
		t.Errorf("❌ GetTempBasePath fallback = %q; want %q", got, want)
	}

	os.Unsetenv("RUNNER_TEMP")
	os.Setenv("TEMP_BASE_PATH", "/basepath")
	want = filepath.Join("/basepath", id)
	got = helpers.GetTempBasePath(id)
	if got != want {
		t.Errorf("❌ GetTempBasePath TEMP_BASE_PATH = %q; want %q", got, want)
	}

	os.Setenv("RUNNER_TEMP", "/runner")
	os.Setenv("TEMP_BASE_PATH", "/basepath")
	want = filepath.Join("/runner", id)
	got = helpers.GetTempBasePath(id)
	if got != want {
		t.Errorf("❌ GetTempBasePath RUNNER_TEMP = %q; want %q", got, want)
	}
}

func TestFlatten(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		prefix   string
		src      map[string]any
		expected map[string]any
	}{
		{
			name:   "Depth1_SimpleMap",
			prefix: "",
			src: map[string]any{
				"a": 1,
				"b": "two",
			},
			expected: map[string]any{
				"a": 1,
				"b": "two",
			},
		},
		{
			name:   "Depth2_NestedMap",
			prefix: "",
			src: map[string]any{
				"a": map[string]any{
					"b": 2,
				},
			},
			expected: map[string]any{
				"a.b": 2,
			},
		},
		{
			name:   "Depth3_NestedMap",
			prefix: "",
			src: map[string]any{
				"a": map[string]any{
					"b": map[string]any{
						"c": 3,
					},
				},
			},
			expected: map[string]any{
				"a.b.c": 3,
			},
		},
		{
			name:   "Depth4_NestedMap",
			prefix: "",
			src: map[string]any{
				"a": map[string]any{
					"b": map[string]any{
						"c": map[string]any{
							"d": 4,
						},
					},
				},
			},
			expected: map[string]any{
				"a.b.c.d": 4,
			},
		},
		{
			name:   "Depth5_NestedMap",
			prefix: "",
			src: map[string]any{
				"a": map[string]any{
					"b": map[string]any{
						"c": map[string]any{
							"d": map[string]any{
								"e": 5,
							},
						},
					},
				},
			},
			expected: map[string]any{
				"a.b.c.d.e": 5,
			},
		},
		{
			name:   "SliceAtRoot",
			prefix: "",
			src: map[string]any{
				"arr": []any{10, 20, 30},
			},
			expected: map[string]any{
				"arr.0": 10,
				"arr.1": 20,
				"arr.2": 30,
			},
		},
		{
			name:   "MixedMapAndSlice",
			prefix: "",
			src: map[string]any{
				"level1": map[string]any{
					"list": []any{
						1,
						map[string]any{"x": 99},
					},
				},
			},
			expected: map[string]any{
				"level1.list.0":   1,
				"level1.list.1.x": 99,
			},
		},
		{
			name:   "NonEmptyPrefix",
			prefix: "root",
			src: map[string]any{
				"foo": map[string]any{
					"bar": []any{true, false},
				},
				"baz": 42,
			},
			expected: map[string]any{
				"root.foo.bar.0": true,
				"root.foo.bar.1": false,
				"root.baz":       42,
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			dest := make(map[string]any)
			helpers.Flatten(tc.prefix, tc.src, dest)

			// Compare length first for quick failure
			if len(dest) != len(tc.expected) {
				t.Fatalf("⛔️ expected %d entries, got %d", len(tc.expected), len(dest))
			}

			// Compare each key/value
			for key, want := range tc.expected {
				got, ok := dest[key]
				if !ok {
					t.Errorf("❌ missing key %q in dest", key)
					continue
				}
				if !reflect.DeepEqual(got, want) {
					t.Errorf("❌ at key %q: expected %#v (%T), got %#v (%T)",
						key, want, want, got, got)
				}
			}

			// Ensure no unexpected keys
			for key := range dest {
				if _, ok := tc.expected[key]; !ok {
					t.Errorf("❌ unexpected key %q in dest", key)
				}
			}
		})
	}
}

func TestSlice(t *testing.T) {
	t.Parallel()

	base := []any{10, 20, 30, 40, 50}

	tests := []struct {
		name               string
		items              []any
		offset, limit      uint64
		wantSlice          []any
		wantStart, wantEnd uint64
	}{
		{
			name:      "EmptyItems_AnyOffsetLimit",
			items:     []any{},
			offset:    0,
			limit:     5,
			wantSlice: nil,
			wantStart: 0,
			wantEnd:   0,
		},
		{
			name:      "OffsetBeyondLen",
			items:     base,
			offset:    10,
			limit:     3,
			wantSlice: nil,
			wantStart: 0,
			wantEnd:   0,
		},
		{
			name:      "OffsetAtLen",
			items:     base,
			offset:    5,
			limit:     1,
			wantSlice: nil,
			wantStart: 0,
			wantEnd:   0,
		},
		{
			name:      "NoLimit_LimitZero",
			items:     base,
			offset:    0,
			limit:     0,
			wantSlice: []any{10, 20, 30, 40, 50},
			wantStart: 0,
			wantEnd:   5,
		},
		{
			name:      "SimpleLimitWithinRange",
			items:     base,
			offset:    0,
			limit:     2,
			wantSlice: []any{10, 20},
			wantStart: 0,
			wantEnd:   2,
		},
		{
			name:      "LimitExceedsLen",
			items:     base,
			offset:    0,
			limit:     10,
			wantSlice: []any{10, 20, 30, 40, 50},
			wantStart: 0,
			wantEnd:   5,
		},
		{
			name:      "MiddlePage_ExactLimit",
			items:     base,
			offset:    1,
			limit:     3,
			wantSlice: []any{20, 30, 40},
			wantStart: 1,
			wantEnd:   4,
		},
		{
			name:      "MiddlePage_LimitExceeds",
			items:     base,
			offset:    3,
			limit:     10,
			wantSlice: []any{40, 50},
			wantStart: 3,
			wantEnd:   5,
		},
		{
			name:      "OffsetNonZero_LimitZeroNoCap",
			items:     base,
			offset:    2,
			limit:     0,
			wantSlice: []any{30, 40, 50},
			wantStart: 2,
			wantEnd:   5,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			gotSlice, gotStart, gotEnd := helpers.Slice(tc.items, tc.offset, tc.limit)

			if !reflect.DeepEqual(gotSlice, tc.wantSlice) {
				t.Errorf("Slice() slice = %v; want %v", gotSlice, tc.wantSlice)
			}
			if gotStart != tc.wantStart {
				t.Errorf("Slice() start = %d; want %d", gotStart, tc.wantStart)
			}
			if gotEnd != tc.wantEnd {
				t.Errorf("Slice() end = %d; want %d", gotEnd, tc.wantEnd)
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
				errCh <- helpers.StreamWriteCSV(path, tt.headers, input)
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

	err := helpers.StreamWriteCSV("/invalid/path/that/does/not/exist.csv", []string{"h1"}, input)
	if err == nil {
		t.Fatal("⛔️ expected error for invalid path, got nil")
	}
}
