package datasources

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/go-mysql-org/go-mysql/schema"
	mysqldriver "gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	gormlogger "gorm.io/gorm/logger"
	gormschema "gorm.io/gorm/schema"
)

const MySQLDefaultIDField = "id"

// generateUniqueServerID creates a unique server ID based on table name
// MySQL server IDs should be between 1 and 4294967295 (2^32-1)
func generateUniqueServerID(tableName string) uint32 {
	// Use hash of table name to generate consistent server ID
	hasher := fnv.New32a()
	hasher.Write([]byte(tableName))
	hashValue := hasher.Sum32()

	// Ensure server ID is in a safe range (100-999999) to avoid conflicts
	serverID := (hashValue % 899900) + 100

	return serverID
}

type MySQLDatasourceTransformer[T any] func(data map[string]any) (T, error)

type MySQLDatasourceFilter struct {
	// SQL WHERE clause (without the "WHERE" keyword)
	// Use "?" for parameters, e.g. "status = ? AND age > ?"
	Query string
	// Query parameters
	Params []any
}

type MySQLDatasourceConfigs[T any] struct {
	// Database connection string
	DSN string
	// Table name to work with
	TableName string
	// GORM model struct for the table.
	// @see: https://gorm.io/docs/models.html
	Model *T
	// ID Field for the table. Default is "id".
	// This would very likely correspond to the primary key field in the table
	IDField string
	// Database flavor: "mysql" (default) or "mariadb"
	DBFlavor string

	// Filter conditions (WHERE clause)
	Filter MySQLDatasourceFilter
	// Sort order (ORDER BY clause)
	Sort map[string]any

	// Provide custom GORM DB instance
	WithDB func(ctx *context.Context, dsn string) (*gorm.DB, error)
	// Provide custom transformer
	WithTransformer MySQLDatasourceTransformer[T]
	// Perform actions on init
	OnInit func(db *gorm.DB) error

	// Binlog replication settings
	DisableReplication  bool
	BinlogStartPosition mysql.Position

	// Connection pool settings
	DBMaxIdleConns    int
	DBMaxOpenConns    int
	DBMaxConnLifetime time.Duration

	// Whether to disable auto-migration of the table schema
	// Use this if you want to manage the table schema manually
	//   or if the table already exists with a different schema
	DisableAutoMigrate bool
}

type MySQLDatasource[T any] struct {
	db        *gorm.DB
	dbFlavor  string
	tableName string

	idField    string
	fieldMap   map[string]*gormschema.Field
	fieldNames []string

	filter      MySQLDatasourceFilter
	sort        map[string]any
	transformer MySQLDatasourceTransformer[T]

	// Replication settings
	disableReplication bool
	serverID           uint32
	binlogPosition     mysql.Position
	canalRunning       bool
	canalMutex         *sync.Mutex

	// Watcher channel
	watcher          chan DatasourcePushRequest
	watcherStartedAt time.Time
	watcherActive    bool
}

// Connect to MySQL using GORM
func ConnectToMySQL[T any](ctx *context.Context, config MySQLDatasourceConfigs[T]) *gorm.DB {
	// Try custom DB
	if config.WithDB != nil {
		db, err := config.WithDB(ctx, config.DSN)
		if err != nil {
			panic(err)
		}
		return db
	}

	// Configure GORM
	gormConfig := &gorm.Config{
		Logger: gormlogger.Default.LogMode(gormlogger.Silent),
	}

	db, err := gorm.Open(mysqldriver.Open(config.DSN), gormConfig)
	if err != nil {
		panic(fmt.Errorf("failed to connect to mysql: %w", err))
	}

	// Test connection
	sqlDB, err := db.DB()
	if err != nil {
		panic(fmt.Errorf("failed to get underlying sql.DB: %w", err))
	}

	if err := sqlDB.PingContext(*ctx); err != nil {
		panic(fmt.Errorf("failed to ping mysql: %w", err))
	}

	// Configure connection pool
	sqlDB.SetMaxIdleConns(max(config.DBMaxIdleConns, 2))
	sqlDB.SetMaxOpenConns(max(config.DBMaxOpenConns, 100))
	sqlDB.SetConnMaxLifetime(max(config.DBMaxConnLifetime, time.Hour))

	return db
}

// NewMySQLDatasource creates and returns a new instance of MySQLDatasource
func NewMySQLDatasource[T any](ctx *context.Context, config MySQLDatasourceConfigs[T]) *MySQLDatasource[T] {
	idField := config.IDField
	if idField == "" {
		idField = MySQLDefaultIDField
	}

	// Ensure model is provided
	if config.Model == nil {
		panic(fmt.Errorf("mysql datasource: model is required"))
	}

	// Connect to MySQL
	db := ConnectToMySQL(ctx, config)

	// Auto-migrate the table
	if !config.DisableAutoMigrate {
		if err := db.Table(config.TableName).AutoMigrate(config.Model); err != nil {
			panic(fmt.Errorf("mysql datasource: failed to auto-migrate table %s: %w", config.TableName, err))
		}
	}

	// Parse the schema for the model struct
	s, err := gormschema.Parse(config.Model, &sync.Map{}, gormschema.NamingStrategy{})
	if err != nil {
		panic("failed to create schema")
	}

	// Get schema field names
	fieldMap := map[string]*gormschema.Field{}
	fieldNames := []string{}
	isValidIDField := false
	for _, field := range s.Fields {
		// Validate ID field
		if field.DBName == idField || field.Name == idField {
			isValidIDField = true
			idField = field.DBName // Use the actual DB column name
		}
		fieldMap[field.DBName] = field
		fieldNames = append(fieldNames, field.DBName)
	}
	if !isValidIDField {
		panic(fmt.Errorf("mysql datasource: invalid ID field '%s'", idField))
	}

	// Validate DB flavor
	flavor := strings.ToLower(config.DBFlavor)
	if flavor == "" {
		flavor = "mysql" // Default to mysql
	}
	if flavor != "mysql" && flavor != "mariadb" {
		panic(fmt.Errorf("mysql datasource: invalid DB flavor '%s'. Supported flavors are: 'mysql', 'mariadb'", config.DBFlavor))
	}

	// Generate unique server ID based on table name hash to avoid conflicts
	serverID := generateUniqueServerID(config.TableName)

	// Set default model if not provided
	ds := &MySQLDatasource[T]{
		db:       db,
		dbFlavor: flavor,

		tableName:  config.TableName,
		idField:    idField,
		fieldMap:   fieldMap,
		fieldNames: fieldNames,

		filter:      config.Filter,
		sort:        config.Sort,
		transformer: config.WithTransformer,

		disableReplication: config.DisableReplication,
		serverID:           serverID,
		binlogPosition:     config.BinlogStartPosition,

		canalRunning: false,
		canalMutex:   new(sync.Mutex),

		watcher:       make(chan DatasourcePushRequest, DefaultWatcherBufferSize),
		watcherActive: false,
	}

	// Initialize data source
	if config.OnInit != nil {
		if err := config.OnInit(ds.db); err != nil {
			panic(err)
		}
	}

	return ds
}

// Marshal GORM Model to map[string]any
func (ds *MySQLDatasource[T]) MarshalModel(model *T) (map[string]any, error) {
	data, err := json.Marshal(model)
	if err != nil {
		return nil, err
	}

	var result map[string]any
	if err := json.Unmarshal(data, &result); err != nil {
		return nil, err
	}

	return result, nil
}

// Unmarshal map[string]any to GORM Model
func (ds *MySQLDatasource[T]) UnmarshalModel(data map[string]any) (*T, error) {
	jsonData, err := json.Marshal(data)
	if err != nil {
		return nil, err
	}

	var result T
	if err := json.Unmarshal(jsonData, &result); err != nil {
		return nil, err
	}

	return &result, nil
}

func (ds *MySQLDatasource[T]) DB() *gorm.DB {
	return ds.db
}

// -------------------------
// Datasource interface
// -------------------------

func (ds *MySQLDatasource[T]) Actions() *DatasourceActionResult {
	return &DatasourceActionResult{
		Read:   true,
		Write:  true,
		Stream: true,
	}
}

// Get total count
func (ds *MySQLDatasource[T]) Count(ctx *context.Context, request *DatasourceFetchRequest) uint64 {
	query := ds.db.Table(ds.tableName)

	// Apply filters
	if ds.filter.Query != "" {
		query = query.Where(ds.filter.Query, ds.filter.Params...)
	}

	// Apply ID filter if provided
	if len(request.IDs) > 0 {
		query = query.Where(fmt.Sprintf("`%s` IN ?", ds.idField), request.IDs)
	}

	var total int64
	if err := query.Count(&total).Error; err != nil {
		return 0
	}

	// Apply offset: if we skip past the end, zero left
	if request.Offset >= uint64(total) {
		return 0
	}
	remaining := uint64(total) - request.Offset

	// Apply size cap: if Size>0 and smaller than what's left, cap it
	if request.Size > 0 && request.Size < remaining {
		return request.Size
	}

	// Otherwise, everything remaining counts
	return remaining
}

// Get data
func (ds *MySQLDatasource[T]) Fetch(ctx *context.Context, request *DatasourceFetchRequest) DatasourceFetchResult {
	query := ds.db.WithContext(*ctx).Table(ds.tableName)

	// Apply filters
	if ds.filter.Query != "" {
		query = query.Where(ds.filter.Query, ds.filter.Params...)
	}

	// Apply sorting
	if len(ds.sort) > 0 {
		for key, direction := range ds.sort {
			orderClause := fmt.Sprintf("`%s`", key)
			if direction == -1 || direction == "desc" || direction == "DESC" {
				orderClause += " DESC"
			} else {
				orderClause += " ASC"
			}
			query = query.Order(orderClause)
		}
	} else {
		// Default sorting by ID field
		query = query.Order(fmt.Sprintf("`%s` ASC", ds.idField))
	}

	// Apply ID filter or pagination
	if len(request.IDs) > 0 {
		query = query.Where(fmt.Sprintf("`%s` IN ?", ds.idField), request.IDs)
	} else {
		if request.Offset > 0 {
			query = query.Offset(int(request.Offset))
		}
		if request.Size > 0 {
			query = query.Limit(int(request.Size))
		}
	}

	// Execute query and get raw results
	var results []T
	if err := query.Find(&results).Error; err != nil {
		return DatasourceFetchResult{
			Err:  fmt.Errorf("mysql fetch error: %w", err),
			Docs: []map[string]any{},
		}
	}

	// Convert results to map[string]any
	var doc map[string]any
	var err error
	docs := make([]map[string]any, len(results))
	for i, result := range results {
		doc, err = ds.MarshalModel(&result)
		if err != nil {
			return DatasourceFetchResult{
				Err:  fmt.Errorf("mysql marshal error: %w", err),
				Docs: []map[string]any{},
			}
		}
		docs[i] = doc
	}

	count := len(docs)
	start := request.Offset
	end := request.Offset + uint64(max(0, count))

	if count == 0 {
		start, end = 0, 0
	}

	return DatasourceFetchResult{
		Start: start,
		End:   end,
		Docs:  docs,
	}
}

// Insert/Update/Delete data
func (ds *MySQLDatasource[T]) Push(ctx *context.Context, request *DatasourcePushRequest) (DatasourcePushCount, error) {
	count := DatasourcePushCount{}
	var pushErr error

	// Start transaction
	tx := ds.db.WithContext(*ctx).Begin(&sql.TxOptions{
		Isolation: sql.LevelReadCommitted,
	})
	defer func() {
		if r := recover(); r != nil {
			tx.Rollback()
			panic(r)
		}
	}()

	// Insert
	if len(request.Inserts) > 0 {
		var item map[string]any
		var row T

		for _, item = range request.Inserts {
			if len(item) == 0 {
				continue
			}

			_, ok := item[ds.idField]
			if !ok {
				pushErr = fmt.Errorf("mysql insert error: missing '%s' field, %#v", ds.idField, item)
				slog.Warn(pushErr.Error())
				continue
			}

			// Transform item to model
			if ds.transformer != nil {
				t, err := ds.transformer(item)
				if err != nil {
					pushErr = fmt.Errorf("mysql insert transformer error: %w", err)
					slog.Warn(pushErr.Error())
					continue
				}
				row = t
			} else {
				t, err := ds.UnmarshalModel(item)
				if err != nil {
					pushErr = fmt.Errorf("mysql insert unmarshal error: %w", err)
					slog.Warn(pushErr.Error())
					continue
				}
				row = *t
			}

			// Use MySQL's ON DUPLICATE KEY UPDATE for upsert
			result := tx.Table(ds.tableName).Clauses(clause.OnConflict{
				Columns:   []clause.Column{{Name: ds.idField}},
				DoUpdates: clause.AssignmentColumns(ds.fieldNames), // TODO: confirm if this only updates available fields
			}).Create(&row)
			if result.Error != nil {
				pushErr = fmt.Errorf("mysql upsert error: %w", result.Error)
				slog.Warn(pushErr.Error())
				continue
			}

			count.Inserts += uint64(result.RowsAffected)
		}
	}

	// Update
	if len(request.Updates) > 0 {
		var item map[string]any
		var row T

		for _, item = range request.Updates {
			if len(item) == 0 {
				continue
			}

			id, ok := item[ds.idField]
			if !ok {
				pushErr = fmt.Errorf("mysql update error: missing '%s' field, %#v", ds.idField, item)
				slog.Warn(pushErr.Error())
				continue
			}

			// Transform item to model
			if ds.transformer != nil {
				t, err := ds.transformer(item)
				if err != nil {
					pushErr = fmt.Errorf("mysql update transformer error: %w", err)
					slog.Warn(pushErr.Error())
					continue
				}
				row = t
			} else {
				t, err := ds.UnmarshalModel(item)
				if err != nil {
					pushErr = fmt.Errorf("mysql update unmarshal error: %w", err)
					slog.Warn(pushErr.Error())
					continue
				}
				row = *t
			}

			// Upsert: Update if exists, insert if not
			result := tx.Table(ds.tableName).
				Where(fmt.Sprintf("`%v` = ?", ds.idField), id).
				Updates(row)
			if result.RowsAffected == 0 {
				if result.Error != nil {
					pushErr = fmt.Errorf("mysql upsert (update) error: %w", result.Error)
					slog.Warn(pushErr.Error())
					continue
				}

				// No rows updated, try insert
				result := tx.Table(ds.tableName).Create(&row)
				if result.Error != nil {
					pushErr = fmt.Errorf("mysql upsert (insert) error: %w", result.Error)
					slog.Warn(pushErr.Error())
				} else {
					count.Updates++
				}
			} else {
				count.Updates++
			}
		}
	}

	// Delete
	if len(request.Deletes) > 0 {
		result := tx.Table(ds.tableName).Where(fmt.Sprintf("`%s` IN ?", ds.idField), request.Deletes).Delete(nil)
		if result.Error != nil {
			pushErr = fmt.Errorf("mysql delete error: %w", result.Error)
			slog.Warn(pushErr.Error())
		} else {
			count.Deletes += uint64(len(request.Deletes))
		}
	}

	// Commit transaction
	if err := tx.Commit().Error; err != nil {
		return count, fmt.Errorf("mysql push error: failed to commit transaction: %w", err)
	}

	return count, pushErr
}

// Listen to binlog replication stream using go-mysql canal
func (ds *MySQLDatasource[T]) Watch(ctx *context.Context, request *DatasourceStreamRequest) <-chan DatasourceStreamResult {

	// If replication is disabled, return empty channel
	if ds.disableReplication {
		panic(fmt.Errorf("mysql watch error: replication is disabled"))
	}

	// Start canal only once, on first watcher
	ds.canalMutex.Lock()
	defer ds.canalMutex.Unlock()
	if !ds.canalRunning {
		ds.canalRunning = true

		canal, err := ds.setupCanal()
		if err != nil {
			panic(fmt.Errorf("mysql watch: failed to setup canal: %w", err))
		}

		// Start canal in background
		go func() {
			defer func() {
				ds.canalMutex.Lock()
				ds.canalRunning = false
				ds.canalMutex.Unlock()
			}()

			err := canal.Run()
			if err != nil {
				slog.Error("mysql watch: canal run failed", "error", err)
			}
		}()

		// Handle canal cleanup
		go func() {
			defer func() {
				ds.canalMutex.Lock()
				if canal != nil && ds.canalRunning {
					canal.Close()
				}
				ds.canalRunning = false
				ds.canalMutex.Unlock()
			}()

			// Wait for context cancellation
			<-(*ctx).Done()
		}()

		ds.watcherActive = true
		ds.watcherStartedAt = time.Now().UTC()
		slog.Info("mysql canal started", "table", ds.tableName)
	}

	// Return stream changes channel
	return StreamChanges(
		ctx,
		fmt.Sprintf("mysql table '%s'", ds.tableName),
		ds.watcher,
		request,
	)
}

// Clear data source
func (ds *MySQLDatasource[T]) Clear(ctx *context.Context) error {

	// Delete all rows from the table
	if err := ds.db.WithContext(*ctx).Exec(fmt.Sprintf("DELETE FROM `%s`", ds.tableName)).Error; err != nil {
		return fmt.Errorf("mysql clear error: %w", err)
	}

	return nil
}

// Close data source
func (ds *MySQLDatasource[T]) Close(ctx *context.Context) error {

	// Close watcher channel
	close(ds.watcher)

	// Close underlying sql.DB
	sqlDB, err := ds.db.DB()
	if err != nil {
		return err
	}

	return sqlDB.Close()
}

// Import data into the data source
func (ds *MySQLDatasource[T]) Import(ctx *context.Context, request DatasourceImportRequest) error {
	switch request.Type {
	case DatasourceImportTypeCSV: // TODO: use mysql native LOAD DATA command
		return LoadCSV(ctx, ds, request.Location, request.BatchSize)
	default:
		return fmt.Errorf("unsupported import type: %s", request.Type)
	}
}

// Export data from the data source
func (ds *MySQLDatasource[T]) Export(ctx *context.Context, request DatasourceExportRequest) error {
	switch request.Type {
	case DatasourceExportTypeCSV: // TODO: use mysql native SELECT INTO OUTFILE command
		return SaveCSV(ctx, ds, request.Location, request.BatchSize)
	default:
		return fmt.Errorf("unsupported export type: %s", request.Type)
	}
}

// ----------------------------------------
// Binlog Replication using go-mysql canal
// ----------------------------------------

// setupCanal sets up the MySQL binlog canal for replication
func (ds *MySQLDatasource[T]) setupCanal() (*canal.Canal, error) {
	// Parse DSN to extract connection parameters
	dsn := ds.getConnectionInfo()

	// Create canal configuration
	cfg := canal.NewDefaultConfig()
	cfg.Addr = dsn.Addr
	cfg.User = dsn.User
	cfg.Password = dsn.Password
	cfg.ServerID = ds.serverID
	cfg.TimestampStringLocation = time.UTC
	cfg.Flavor = ds.dbFlavor // "mysql" or "mariadb"
	cfg.DiscardNoMetaRowEvent = true
	cfg.Dump = canal.DumpConfig{
		IgnoreTables: []string{ds.tableName}, // Ignore during initial dump
	}

	// Set include tables
	cfg.IncludeTableRegex = []string{fmt.Sprintf(".*\\.%s", ds.tableName)}

	// Create canal instance
	c, err := canal.NewCanal(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create canal: %w", err)
	}

	// Set event handler
	c.SetEventHandler(&MySQLEventHandler[T]{
		ds: ds,
	})

	return c, nil
}

// MySQLConnectionInfo holds connection details parsed from DSN
type MySQLConnectionInfo struct {
	Addr     string
	User     string
	Password string
	Database string
}

// getConnectionInfo extracts connection information from the GORM dialector
func (ds *MySQLDatasource[T]) getConnectionInfo() MySQLConnectionInfo {
	// Extract DSN from the GORM dialector
	if dialector, ok := ds.db.Dialector.(*mysqldriver.Dialector); ok {
		config := dialector.DSNConfig
		return MySQLConnectionInfo{
			Addr:     config.Addr,
			User:     config.User,
			Password: config.Passwd,
			Database: config.DBName,
		}
	}
	panic("failed to extract DSN from GORM dialector")
}

// MySQLEventHandler handles MySQL binlog events
type MySQLEventHandler[T any] struct {
	ds *MySQLDatasource[T] // Use any since we can't use generics in struct embedding
}

// OnRow handles row-level binlog events
func (h *MySQLEventHandler[T]) OnRow(e *canal.RowsEvent) error {
	// Check if this event is for our table
	if e.Table.Name != h.ds.tableName {
		return nil
	}

	// Check if this event is for our table
	if e.Table.Name != h.ds.tableName {
		return nil
	}

	// Check if watcher started and event timestamp is after watcher started
	if !h.ds.watcherActive || h.ds.watcher == nil || h.ds.watcherStartedAt.IsZero() || int64(e.Header.Timestamp) < h.ds.watcherStartedAt.Unix() {
		return nil
	}

	slog.Debug("mysql binlog event", "table", e.Table.Name, "action", e.Action, "rows", len(e.Rows))

	var pushRequest DatasourcePushRequest

	switch e.Action {
	case canal.InsertAction:
		for _, row := range e.Rows {
			data := h.parseRow(e.Table, row)
			if data != nil {
				pushRequest.Inserts = append(pushRequest.Inserts, data)
			}
		}

	case canal.UpdateAction:
		// For updates, e.Rows contains [oldRow, newRow, oldRow, newRow, ...]
		for i := 1; i < len(e.Rows); i += 2 {
			data := h.parseRow(e.Table, e.Rows[i]) // Use new row data
			if data != nil {
				pushRequest.Updates = append(pushRequest.Updates, data)
			}
		}

	case canal.DeleteAction:
		for _, row := range e.Rows {
			data := h.parseRow(e.Table, row)
			if data != nil {
				if id, ok := data[h.ds.idField]; ok {
					pushRequest.Deletes = append(pushRequest.Deletes, fmt.Sprintf("%v", id))
				}
			}
		}
	}

	// Send to watcher
	if len(pushRequest.Inserts)+len(pushRequest.Updates)+len(pushRequest.Deletes) > 0 {
		slog.Info("mysql sending to watchers", "table", e.Table.Name,
			"inserts", len(pushRequest.Inserts),
			"updates", len(pushRequest.Updates),
			"deletes", len(pushRequest.Deletes),
		)

		h.ds.watcher <- pushRequest
	}

	return nil
}

// OnTableChanged handles table structure changes
func (h *MySQLEventHandler[T]) OnTableChanged(header *replication.EventHeader, schema, table string) error {
	return nil
}

// OnDDL handles DDL events
func (h *MySQLEventHandler[T]) OnDDL(header *replication.EventHeader, nextPos mysql.Position, queryEvent *replication.QueryEvent) error {
	return nil
}

// OnRotate handles binlog rotation events
func (h *MySQLEventHandler[T]) OnRotate(header *replication.EventHeader, e *replication.RotateEvent) error {
	// Update position
	h.ds.binlogPosition = mysql.Position{
		Name: string(e.NextLogName),
		Pos:  uint32(e.Position),
	}
	return nil
}

// OnPosSynced handles position sync events
func (h *MySQLEventHandler[T]) OnPosSynced(header *replication.EventHeader, pos mysql.Position, set mysql.GTIDSet, force bool) error {
	h.ds.binlogPosition = pos
	return nil
}

// OnXID handles transaction commit events
func (h *MySQLEventHandler[T]) OnXID(header *replication.EventHeader, nextPos mysql.Position) error {
	// Update position
	h.ds.binlogPosition = nextPos
	return nil
}

// OnGTID handles GTID events
func (h *MySQLEventHandler[T]) OnGTID(header *replication.EventHeader, gtid mysql.BinlogGTIDEvent) error {
	return nil
}

// OnRowsQueryEvent handles rows query events
func (h *MySQLEventHandler[T]) OnRowsQueryEvent(e *replication.RowsQueryEvent) error {
	return nil
}

// String returns string representation
func (h *MySQLEventHandler[T]) String() string {
	return "MySQLEventHandler"
}

// parseRow converts a binlog row to map[string]any
func (h *MySQLEventHandler[T]) parseRow(table *schema.Table, row []any) map[string]any {
	if len(row) != len(table.Columns) {
		return nil
	}

	result := make(map[string]any)
	for i, col := range table.Columns {
		if i < len(row) {
			v := row[i]
			if v == nil {
				result[col.Name] = nil
				continue
			}

			// Try to parse using GORM field type if available
			if field, ok := h.ds.fieldMap[col.Name]; ok {
				if v, err := ParseGormFieldValue(field, v); err == nil {
					result[col.Name] = v
					continue
				}
			}

			// Fallback to raw value
			result[col.Name] = v
		}
	}

	return result
}
