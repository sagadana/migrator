package helpers

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
)

// jobCall records one invocation of the user-supplied fn.
type jobCall struct {
	ID     uint64
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
		config   ParallelBatchConfig
		expected []jobCall
	}{
		{
			name: "Basic single worker, partial last batch",
			config: ParallelBatchConfig{
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
			config: ParallelBatchConfig{
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
			config: ParallelBatchConfig{
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
			config: ParallelBatchConfig{
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
			config: ParallelBatchConfig{
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
			config: ParallelBatchConfig{Units: 5, Total: 0, BatchSize: 3},
			// expected empty slice
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			calls := make([]jobCall, 0)
			mu := new(sync.Mutex)

			// Wrap the fn to capture calls
			ParallelBatch(&ctx, &tt.config, func(c *context.Context, id, size, offset uint64) {
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
		got := ExtractNumber(tt.input)
		if got != tt.want {
			t.Errorf("❌ ExtractNumber(%q) = %d; want %d", tt.input, got, tt.want)
		}
	}
}

func TestNumberAwareSort(t *testing.T) {
	t.Parallel()

	data := []string{"file10.txt", "file2.txt", "file1.txt", "file2a.txt", "file2b.txt"}
	NumberAwareSort(data)
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
	s1 := RandomString(length)
	s2 := RandomString(length)
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

	logger := CreateTextLogger(slog.LevelDebug)
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

	logger = CreateTextLogger(slog.LevelError)
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
	got := GetTempBasePath(id)
	if got != want {
		t.Errorf("❌ GetTempBasePath fallback = %q; want %q", got, want)
	}

	os.Unsetenv("RUNNER_TEMP")
	os.Setenv("TEMP_BASE_PATH", "/basepath")
	want = filepath.Join("/basepath", id)
	got = GetTempBasePath(id)
	if got != want {
		t.Errorf("❌ GetTempBasePath TEMP_BASE_PATH = %q; want %q", got, want)
	}

	os.Setenv("RUNNER_TEMP", "/runner")
	os.Setenv("TEMP_BASE_PATH", "/basepath")
	want = filepath.Join("/runner", id)
	got = GetTempBasePath(id)
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
			Flatten(tc.prefix, tc.src, dest)

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

func TestMarshalModel(t *testing.T) {
	t.Parallel()

	type SimpleStruct struct {
		Name  string `json:"name"`
		Age   int    `json:"age"`
		Email string `json:"email"`
	}

	type NestedStruct struct {
		ID      int          `json:"id"`
		Profile SimpleStruct `json:"profile"`
		Tags    []string     `json:"tags"`
	}

	tests := []struct {
		name      string
		model     any
		expected  map[string]any
		wantError bool
	}{
		{
			name: "SimpleStruct",
			model: &SimpleStruct{
				Name:  "John Doe",
				Age:   30,
				Email: "john@example.com",
			},
			expected: map[string]any{
				"name":  "John Doe",
				"age":   float64(30), // JSON numbers unmarshal to float64
				"email": "john@example.com",
			},
			wantError: false,
		},
		{
			name: "NestedStruct",
			model: &NestedStruct{
				ID: 1,
				Profile: SimpleStruct{
					Name:  "Jane Smith",
					Age:   25,
					Email: "jane@example.com",
				},
				Tags: []string{"developer", "golang"},
			},
			expected: map[string]any{
				"id": float64(1),
				"profile": map[string]any{
					"name":  "Jane Smith",
					"age":   float64(25),
					"email": "jane@example.com",
				},
				"tags": []any{"developer", "golang"},
			},
			wantError: false,
		},
		{
			name: "EmptyStruct",
			model: &SimpleStruct{
				Name:  "",
				Age:   0,
				Email: "",
			},
			expected: map[string]any{
				"name":  "",
				"age":   float64(0),
				"email": "",
			},
			wantError: false,
		},
		{
			name: "NilSliceInStruct",
			model: &NestedStruct{
				ID:      99,
				Profile: SimpleStruct{Name: "Test"},
				Tags:    nil,
			},
			expected: map[string]any{
				"id": float64(99),
				"profile": map[string]any{
					"name":  "Test",
					"age":   float64(0),
					"email": "",
				},
				"tags": nil,
			},
			wantError: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var result map[string]any
			var err error

			switch v := tc.model.(type) {
			case *SimpleStruct:
				result, err = MarshalModel(v)
			case *NestedStruct:
				result, err = MarshalModel(v)
			}

			if tc.wantError {
				if err == nil {
					t.Fatalf("⛔️ expected error but got none")
				}
				return
			}

			if err != nil {
				t.Fatalf("⛔️ unexpected error: %v", err)
			}

			if !reflect.DeepEqual(result, tc.expected) {
				t.Errorf("❌ MarshalModel() = %#v; want %#v", result, tc.expected)
			}
		})
	}
}

func TestUnmarshalModel(t *testing.T) {
	t.Parallel()

	type SimpleStruct struct {
		Name  string `json:"name"`
		Age   int    `json:"age"`
		Email string `json:"email"`
	}

	type NestedStruct struct {
		ID      int          `json:"id"`
		Profile SimpleStruct `json:"profile"`
		Tags    []string     `json:"tags"`
	}

	tests := []struct {
		name      string
		data      map[string]any
		checkFunc func(t *testing.T, result any)
		wantError bool
	}{
		{
			name: "SimpleStruct",
			data: map[string]any{
				"name":  "John Doe",
				"age":   float64(30),
				"email": "john@example.com",
			},
			checkFunc: func(t *testing.T, result any) {
				model := result.(*SimpleStruct)
				if model.Name != "John Doe" {
					t.Errorf("❌ Name = %q; want %q", model.Name, "John Doe")
				}
				if model.Age != 30 {
					t.Errorf("❌ Age = %d; want %d", model.Age, 30)
				}
				if model.Email != "john@example.com" {
					t.Errorf("❌ Email = %q; want %q", model.Email, "john@example.com")
				}
			},
			wantError: false,
		},
		{
			name: "NestedStruct",
			data: map[string]any{
				"id": float64(1),
				"profile": map[string]any{
					"name":  "Jane Smith",
					"age":   float64(25),
					"email": "jane@example.com",
				},
				"tags": []any{"developer", "golang"},
			},
			checkFunc: func(t *testing.T, result any) {
				model := result.(*NestedStruct)
				if model.ID != 1 {
					t.Errorf("❌ ID = %d; want %d", model.ID, 1)
				}
				if model.Profile.Name != "Jane Smith" {
					t.Errorf("❌ Profile.Name = %q; want %q", model.Profile.Name, "Jane Smith")
				}
				if model.Profile.Age != 25 {
					t.Errorf("❌ Profile.Age = %d; want %d", model.Profile.Age, 25)
				}
				if len(model.Tags) != 2 {
					t.Errorf("❌ len(Tags) = %d; want %d", len(model.Tags), 2)
				}
				if model.Tags[0] != "developer" || model.Tags[1] != "golang" {
					t.Errorf("❌ Tags = %v; want [developer golang]", model.Tags)
				}
			},
			wantError: false,
		},
		{
			name: "EmptyData",
			data: map[string]any{},
			checkFunc: func(t *testing.T, result any) {
				model := result.(*SimpleStruct)
				if model.Name != "" || model.Age != 0 || model.Email != "" {
					t.Errorf("❌ expected zero values, got %+v", model)
				}
			},
			wantError: false,
		},
		{
			name: "PartialData",
			data: map[string]any{
				"name": "Partial User",
			},
			checkFunc: func(t *testing.T, result any) {
				model := result.(*SimpleStruct)
				if model.Name != "Partial User" {
					t.Errorf("❌ Name = %q; want %q", model.Name, "Partial User")
				}
				if model.Age != 0 {
					t.Errorf("❌ Age = %d; want %d", model.Age, 0)
				}
				if model.Email != "" {
					t.Errorf("❌ Email = %q; want empty string", model.Email)
				}
			},
			wantError: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var err error
			var result any

			// Determine which type to unmarshal to based on data structure
			if _, hasProfile := tc.data["profile"]; hasProfile {
				result, err = UnmarshalModel[NestedStruct](tc.data)
			} else {
				result, err = UnmarshalModel[SimpleStruct](tc.data)
			}

			if tc.wantError {
				if err == nil {
					t.Fatalf("⛔️ expected error but got none")
				}
				return
			}

			if err != nil {
				t.Fatalf("⛔️ unexpected error: %v", err)
			}

			if result == nil {
				t.Fatalf("⛔️ result is nil")
			}

			tc.checkFunc(t, result)
		})
	}
}

func TestMarshalUnmarshalRoundTrip(t *testing.T) {
	t.Parallel()

	type TestStruct struct {
		ID       int               `json:"id"`
		Name     string            `json:"name"`
		Active   bool              `json:"active"`
		Score    float64           `json:"score"`
		Tags     []string          `json:"tags"`
		Metadata map[string]string `json:"metadata"`
	}

	original := &TestStruct{
		ID:     42,
		Name:   "Test Item",
		Active: true,
		Score:  98.5,
		Tags:   []string{"important", "verified"},
		Metadata: map[string]string{
			"key1": "value1",
			"key2": "value2",
		},
	}

	// Marshal to map
	marshaled, err := MarshalModel(original)
	if err != nil {
		t.Fatalf("⛔️ MarshalModel failed: %v", err)
	}

	// Unmarshal back to struct
	unmarshaled, err := UnmarshalModel[TestStruct](marshaled)
	if err != nil {
		t.Fatalf("⛔️ UnmarshalModel failed: %v", err)
	}

	// Compare
	if unmarshaled.ID != original.ID {
		t.Errorf("❌ ID = %d; want %d", unmarshaled.ID, original.ID)
	}
	if unmarshaled.Name != original.Name {
		t.Errorf("❌ Name = %q; want %q", unmarshaled.Name, original.Name)
	}
	if unmarshaled.Active != original.Active {
		t.Errorf("❌ Active = %v; want %v", unmarshaled.Active, original.Active)
	}
	if unmarshaled.Score != original.Score {
		t.Errorf("❌ Score = %f; want %f", unmarshaled.Score, original.Score)
	}
	if !reflect.DeepEqual(unmarshaled.Tags, original.Tags) {
		t.Errorf("❌ Tags = %v; want %v", unmarshaled.Tags, original.Tags)
	}
	if !reflect.DeepEqual(unmarshaled.Metadata, original.Metadata) {
		t.Errorf("❌ Metadata = %v; want %v", unmarshaled.Metadata, original.Metadata)
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

			gotSlice, gotStart, gotEnd := Slice(tc.items, tc.offset, tc.limit)

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
