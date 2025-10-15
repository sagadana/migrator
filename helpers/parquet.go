package helpers

import (
	"log"
	"log/slog"
	"os"
	"path/filepath"
	"reflect"
	"strings"

	"github.com/parquet-go/parquet-go"
)

// reflectTypeToParquetNode converts a reflect.Type to a parquet.Node recursively
// This function handles primitive types, arrays, slices, and nested structures
func reflectTypeToParquetNode(fieldType reflect.Type) parquet.Node {
	switch fieldType.Kind() {
	case reflect.Bool:
		return parquet.Leaf(parquet.BooleanType)

	case reflect.Int8:
		return parquet.Int(8)
	case reflect.Int16:
		return parquet.Int(16)
	case reflect.Int, reflect.Int32:
		return parquet.Int(32)
	case reflect.Int64:
		return parquet.Int(64)

	case reflect.Uint8:
		return parquet.Uint(8)
	case reflect.Uint16:
		return parquet.Uint(16)
	case reflect.Uint, reflect.Uint32:
		return parquet.Uint(32)
	case reflect.Uint64:
		return parquet.Uint(64)

	case reflect.Float32:
		return parquet.Leaf(parquet.FloatType)
	case reflect.Float64:
		return parquet.Leaf(parquet.DoubleType)

	case reflect.String:
		return parquet.String()

	case reflect.Slice, reflect.Array:
		// Handle byte arrays ([]byte) as ByteArray type
		if fieldType.Elem().Kind() == reflect.Uint8 {
			return parquet.Leaf(parquet.ByteArrayType)
		}
		// Recursively handle element type for arrays/slices
		return parquet.List(reflectTypeToParquetNode(fieldType.Elem()))

	case reflect.Map:
		// Handle maps as a JSON-encoded string fallback
		return parquet.Map(parquet.String(), reflectTypeToParquetNode(fieldType.Elem()))

	case reflect.Struct:
		// Handle structs as groups (nested structures)
		// For now, fallback to ByteArray (JSON-encoded)
		// Could be extended to recursively build nested groups
		return parquet.JSON()

	default:
		// Fallback for unsupported types - use ByteArray
		return parquet.Leaf(parquet.ByteArrayType)
	}
}

func getSchemaNameFromPath(path string) string {
	// Get base name without directory and extension from path
	baseName := strings.TrimSuffix(filepath.Base(path), filepath.Ext(path))
	// Replace invalid characters for Parquet schema name
	return strings.Map(func(r rune) rune {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '_' {
			return r
		}
		return '_'
	}, baseName)
}

// Read streams of a Parquet File
func StreamReadParquet(path string, batchSize uint64) (<-chan []map[string]any, error) {
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

		// Get file info for size
		fileInfo, err := file.Stat()
		if err != nil || fileInfo.Size() == 0 {
			slog.Warn("error reading file info:", "error", err)
			return
		}

		// Open Parquet file
		pf, err := parquet.OpenFile(file, fileInfo.Size())
		if err != nil || pf.Size() == 0 || len(pf.Schema().Fields()) == 0 {
			slog.Warn("error opening parquet file:", "error", err)
			return
		}

		// Create a reader
		reader := parquet.NewReader(pf)
		defer reader.Close()

		// Get schema to extract column names
		schema := reader.Schema()

		// Get column definitions
		var columnNames []string
		for _, field := range schema.Fields() {
			columnNames = append(columnNames, field.Name())
		}

		// Batch
		records := make([]map[string]any, 0)

		// Reusable vars
		var row parquet.Row
		var val parquet.Value
		var n, i, colIdx int

		// Read rows from Parquet file using Row API
		rows := make([]parquet.Row, batchSize)
		for {
			n, err = reader.ReadRows(rows)
			if n > 0 {

				// Convert parquet.Row to map[string]any
				for i = range n {
					record := make(map[string]any)
					if i < len(rows) {
						row = rows[i]

						// Reconstruct row into a map using schema
						colIdx = 0
						for _, val = range row {
							if colIdx < len(columnNames) {
								// Convert parquet value to Go value based on kind
								switch val.Kind() {
								case parquet.Boolean:
									record[columnNames[colIdx]] = val.Boolean()
								case parquet.Int32:
									record[columnNames[colIdx]] = val.Int32()
								case parquet.Int64:
									record[columnNames[colIdx]] = val.Int64()
								case parquet.Float:
									record[columnNames[colIdx]] = val.Float()
								case parquet.Double:
									record[columnNames[colIdx]] = val.Double()
								case parquet.ByteArray, parquet.FixedLenByteArray:
									// Convert byte array to string for simplicity
									record[columnNames[colIdx]] = string(val.ByteArray())
								default:
									// Use String() method for other types
									record[columnNames[colIdx]] = val.String()
								}
								colIdx++
							}
						}

						records = append(records, record)
					}

					// Send batch when full
					if len(records) >= int(batchSize) {
						out <- records
						records = make([]map[string]any, 0)
					}
				}
			}

			if err != nil {
				if err.Error() == "EOF" {
					break // End of file
				}
				log.Println("Error reading record:", err)
				return
			}
		}

		// Write remaining records
		if len(records) > 0 {
			out <- records
		}
	}()

	return out, nil
}

// Save stream of contents to Parquet file
func StreamWriteParquet(path string, fields map[string]reflect.Type, input <-chan []map[string]any) error {
	// Create or truncate the file
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()

	// Build Parquet schema from fields using the recursive conversion function
	schemaGroup := make(parquet.Group)
	for fieldName, fieldType := range fields {
		// Convert reflect.Type to parquet.Node and wrap with Optional
		schemaGroup[fieldName] = parquet.Optional(reflectTypeToParquetNode(fieldType))
	}
	// Get schema name
	baseName := getSchemaNameFromPath(path)
	schema := parquet.NewSchema(baseName, schemaGroup)

	// Get the column order from the schema (Group sorts by name)
	schemaColumns := schema.Columns()
	columnOrder := make([]string, len(schemaColumns))
	for i, col := range schemaColumns {
		if len(col) > 0 {
			columnOrder[i] = col[0] // Get the column name
		}
	}

	// Create Parquet writer with explicit schema
	writer := parquet.NewWriter(file, schema)

	// Reusable vars
	var rows []parquet.Row
	var row parquet.Row
	var val any
	var colName string
	var i, j int
	var records []map[string]any
	var record map[string]any

	// Write records from input channel
	for records = range input {
		// Convert map[string]any records to parquet.Row
		rows = make([]parquet.Row, len(records))
		for i, record = range records {
			// Build row matching schema column order
			row = make(parquet.Row, len(columnOrder))
			for j, colName = range columnOrder {
				val = record[colName]
				if val != nil {
					// Convert value to parquet.Value
					row[j] = parquet.ValueOf(val).Level(0, 1, j)
				} else {
					// Use null for missing values - optional fields have max definition level 1
					row[j] = parquet.Value{}.Level(0, 0, j)
				}
			}
			rows[i] = row
		}

		// Write rows
		_, err = writer.WriteRows(rows)
		if err != nil {
			return err
		}
	}

	return writer.Close()
}
