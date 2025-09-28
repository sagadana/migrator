package datasources

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/lib/pq"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

const PostgresDefaultIDField = "id"

type PostgresDatasourceTransformer[T any] func(data map[string]any) (T, error)

type PostgresDatasourceFilter struct {
	// SQL WHERE clause (without the "WHERE" keyword)
	// Use "?" for parameters, e.g. "status = ? AND age > ?"
	Query string
	// Query parameters
	Params []any
}

type PostgresDatasourceConfigs[T any] struct {
	// Database connection string
	DSN string
	// Table name to work with
	TableName string
	// GORM model struct for the table
	// @see: https://gorm.io/docs/models.html
	Model *T
	// ID Field for input JSON. Default is "id"
	IDField string

	// Filter conditions (WHERE clause)
	Filter PostgresDatasourceFilter
	// Sort order (ORDER BY clause)
	Sort map[string]any

	// Provide custom GORM DB instance
	WithDB func(ctx *context.Context, dsn string) (*gorm.DB, error)
	// Provide custom transformer
	WithTransformer PostgresDatasourceTransformer[T]
	// Perform actions on init
	OnInit func(db *gorm.DB) error

	// Logical replication settings
	PublicationName     string
	ReplicationSlotName string
	DisableReplication  bool
}

type PostgresDatasource[T any] struct {
	db        *gorm.DB
	tableName string
	model     *T
	idField   string

	filter      PostgresDatasourceFilter
	sort        map[string]any
	transformer PostgresDatasourceTransformer[T]

	// Replication settings
	publicationName     string
	replicationSlotName string
	disableReplication  bool
}

// Connect to PostgreSQL using GORM
func ConnectToPostgreSQL[T any](ctx *context.Context, config PostgresDatasourceConfigs[T]) *gorm.DB {
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
		Logger: logger.Default.LogMode(logger.Silent),
	}

	db, err := gorm.Open(postgres.Open(config.DSN), gormConfig)
	if err != nil {
		panic(fmt.Errorf("failed to connect to postgres: %w", err))
	}

	// Test connection
	sqlDB, err := db.DB()
	if err != nil {
		panic(fmt.Errorf("failed to get underlying sql.DB: %w", err))
	}

	if err := sqlDB.PingContext(*ctx); err != nil {
		panic(fmt.Errorf("failed to ping postgres: %w", err))
	}

	// Configure connection pool
	// TODO: Make these configurable
	sqlDB.SetMaxIdleConns(10)
	sqlDB.SetMaxOpenConns(100)
	sqlDB.SetConnMaxLifetime(time.Hour)

	return db
}

// NewPostgresDatasource creates and returns a new instance of PostgresDatasource
func NewPostgresDatasource[T any](ctx *context.Context, config PostgresDatasourceConfigs[T]) *PostgresDatasource[T] {
	idField := config.IDField
	if idField == "" {
		idField = PostgresDefaultIDField
	}

	tableSuffix := strings.ReplaceAll(strings.ToLower(strings.TrimSpace(config.TableName)), "-", "_")

	// Set default publication names
	publicationName := config.PublicationName
	if publicationName == "" {
		publicationName = fmt.Sprintf("%s_pub", tableSuffix)
	}

	// Set default replication slot name
	replicationSlotName := config.ReplicationSlotName
	if replicationSlotName == "" {
		replicationSlotName = fmt.Sprintf("%s_slot", tableSuffix)
	}

	// Ensure model is provided
	if config.Model == nil {
		panic(fmt.Errorf("postgres datasource: model is required"))
	}

	// Set default model if not provided
	ds := &PostgresDatasource[T]{
		db:        ConnectToPostgreSQL(ctx, config),
		tableName: config.TableName,
		model:     config.Model,
		idField:   idField,

		filter:      config.Filter,
		sort:        config.Sort,
		transformer: config.WithTransformer,

		publicationName:     publicationName,
		replicationSlotName: replicationSlotName,
		disableReplication:  config.DisableReplication,
	}

	// Auto-migrate the table
	if err := ds.db.Table(config.TableName).AutoMigrate(config.Model); err != nil {
		panic(fmt.Errorf("postgres datasource: failed to auto-migrate table %s: %w", config.TableName, err))
	}

	// Set up logical replication if not disabled
	if !ds.disableReplication {

		// Check logical replication
		qCtx, qCancel := context.WithTimeout(*ctx, 10*time.Second)
		defer qCancel()
		if err := ds.checkLogicalReplication(&qCtx); err != nil {
			panic(fmt.Errorf("postgres datasource: %w", err))
		}

		// Create publication (no subscription needed since we use pglogrepl directly)
		qCtx, qCancel = context.WithTimeout(*ctx, 10*time.Second)
		defer qCancel()
		if err := ds.createPublication(&qCtx); err != nil {
			panic(fmt.Errorf("postgres datasource: failed to create publication: %w", err))
		}

		// Note: We don't create a subscription since we use pglogrepl directly
		slog.Info("Logical replication setup complete", "publication", ds.publicationName, "table", ds.tableName)
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
func (ds *PostgresDatasource[T]) MarshalModel(model *T) (map[string]any, error) {
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
func (ds *PostgresDatasource[T]) UnmarshalModel(data map[string]any) (*T, error) {
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

func (ds *PostgresDatasource[T]) DB() *gorm.DB {
	return ds.db
}

// -------------------------
// Datasource interface
// -------------------------

// Get total count
func (ds *PostgresDatasource[T]) Count(ctx *context.Context, request *DatasourceFetchRequest) uint64 {
	query := ds.db.Table(ds.tableName)

	// Apply filters
	if ds.filter.Query != "" {
		query = query.Where(ds.filter.Query, ds.filter.Params...)
	}

	// Apply ID filter if provided
	if len(request.IDs) > 0 {
		query = query.Where(fmt.Sprintf("%s IN ?", pq.QuoteIdentifier(ds.idField)), request.IDs)
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
func (ds *PostgresDatasource[T]) Fetch(ctx *context.Context, request *DatasourceFetchRequest) DatasourceFetchResult {
	query := ds.db.WithContext(*ctx).Table(ds.tableName)

	// Apply filters
	if ds.filter.Query != "" {
		query = query.Where(ds.filter.Query, ds.filter.Params...)
	}

	// Apply sorting
	if len(ds.sort) > 0 {
		for key, direction := range ds.sort {
			orderClause := key
			if direction == -1 || direction == "desc" || direction == "DESC" {
				orderClause = pq.QuoteIdentifier(orderClause) + " DESC"
			} else {
				orderClause = pq.QuoteIdentifier(orderClause) + " ASC"
			}
			query = query.Order(orderClause)
		}
	} else {
		// Default sorting by ID field
		query = query.Order(pq.QuoteIdentifier(ds.idField) + " ASC")
	}

	// Apply ID filter or pagination
	if len(request.IDs) > 0 {
		query = query.Where(fmt.Sprintf("%s IN ?", pq.QuoteIdentifier(ds.idField)), request.IDs)
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
			Err:  fmt.Errorf("postgres fetch error: %w", err),
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
				Err:  fmt.Errorf("postgres marshal error: %w", err),
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
func (ds *PostgresDatasource[T]) Push(ctx *context.Context, request *DatasourcePushRequest) (DatasourcePushCount, error) {
	count := DatasourcePushCount{}
	var pushErr error

	// Start transaction
	tx := ds.db.WithContext(*ctx).Begin()
	defer func() {
		if r := recover(); r != nil {
			tx.Rollback()
			panic(r)
		}
	}()

	// Insert
	if len(request.Inserts) > 0 {
		var item map[string]any
		var trans T
		var err error

		for _, item = range request.Inserts {
			if item == nil {
				continue
			}

			_, ok := item[ds.idField]
			if !ok {
				pushErr = fmt.Errorf("postgres insert error: missing '%s' field", ds.idField)
				continue
			}

			// Transform
			if ds.transformer != nil {
				trans, err = ds.transformer(item)
				if err != nil {
					pushErr = fmt.Errorf("postgres insert transformer error: %w", err)
					continue
				}

				// Create record
				if err = tx.Table(ds.tableName).Create(&trans).Error; err != nil {
					pushErr = fmt.Errorf("postgres insert error: %w", err)
					continue
				}
			} else if err = tx.Table(ds.tableName).Create(&item).Error; err != nil {
				pushErr = fmt.Errorf("postgres insert error: %w", err)
				continue
			}

			count.Inserts++
		}
	}

	// Update
	if len(request.Updates) > 0 {
		var item map[string]any
		var trans T
		var err error

		for _, item = range request.Updates {
			if item == nil {
				continue
			}

			// Transform
			if ds.transformer != nil {
				trans, err = ds.transformer(item)
				if err != nil {
					pushErr = fmt.Errorf("postgres update transformer error: %w", err)
					continue
				}

				// Upsert: Insert if not exists, otherwise update
				var existingModel T
				err := tx.Table(ds.tableName).Where(fmt.Sprintf("%s = ?", pq.QuoteIdentifier(ds.idField)), item[ds.idField]).First(&existingModel).Error
				if err != nil && err == gorm.ErrRecordNotFound {
					// Record doesn't exist, insert it
					if err = tx.Table(ds.tableName).Create(&trans).Error; err != nil {
						pushErr = fmt.Errorf("postgres upsert insert error: %w", err)
						continue
					}

					count.Updates++
				} else if err != nil {
					pushErr = fmt.Errorf("postgres upsert query error: %w", err)
					continue
				} else {
					// Record exists, update it
					result := tx.Table(ds.tableName).Where(fmt.Sprintf("%s = ?", pq.QuoteIdentifier(ds.idField)), item[ds.idField]).Updates(trans)
					if result.Error != nil {
						pushErr = fmt.Errorf("postgres upsert update error: %w", result.Error)
						continue
					}

					count.Updates += uint64(result.RowsAffected)
				}

			} else {

				// Update record
				result := tx.Table(ds.tableName).Updates(item)
				if result.Error != nil {
					pushErr = fmt.Errorf("postgres update error: %w", result.Error)
					continue
				}

				count.Updates += uint64(result.RowsAffected)
			}
		}
	}

	// Delete
	if len(request.Deletes) > 0 {
		result := tx.Table(ds.tableName).Where(fmt.Sprintf("%s IN ?", ds.idField), request.Deletes).Delete(nil)
		if result.Error != nil {
			pushErr = fmt.Errorf("postgres delete error: %w", result.Error)
		} else {
			count.Deletes += uint64(len(request.Deletes))
		}
	}

	// Commit transaction
	if err := tx.Commit().Error; err != nil {
		return count, fmt.Errorf("postgres push error: failed to commit transaction: %w", err)
	}

	return count, pushErr
}

// Listen to logical replication stream using pgx replication protocol
func (ds *PostgresDatasource[T]) Watch(ctx *context.Context, request *DatasourceStreamRequest) <-chan DatasourceStreamResult {

	// If replication is disabled, end
	if ds.disableReplication {
		panic(fmt.Errorf("postgres watch error: replication is disabled"))
	}

	watcher := make(chan DatasourcePushRequest)

	// Process PostgreSQL logical replication events
	processEvent := func(event *ReplicationEvent) DatasourcePushRequest {
		inserts, updates, deletes := []map[string]any{}, []map[string]any{}, []string{}

		switch event.Action {
		case ReplicationEventActionInsert:
			if event.Data != nil {
				inserts = append(inserts, event.Data)
			}

		case ReplicationEventActionUpdate:
			if event.Data != nil {
				updates = append(updates, event.Data)
			}

		case ReplicationEventActionDelete:
			if event.Data != nil {
				if id, ok := event.Data[ds.idField]; ok {
					deletes = append(deletes, fmt.Sprintf("%+v", id))
				}
			}
		}

		return DatasourcePushRequest{
			Inserts: inserts,
			Updates: updates,
			Deletes: deletes,
		}
	}

	// Start logical replication listener in background
	go func(bgCtx context.Context) {
		defer close(watcher)

		// Create replication connection using pgx
		err := ds.startLogicalReplication(bgCtx, watcher, processEvent)
		if err != nil {
			slog.Error("postgres watch: logical replication failed", "error", err)
			return
		}
	}(*ctx)

	// Return stream changes channel
	return StreamChanges(
		ctx,
		fmt.Sprintf("postgres table '%s'", ds.tableName),
		watcher,
		request,
	)
}

// Clear data source
func (ds *PostgresDatasource[T]) Clear(ctx *context.Context) error {
	if err := ds.db.WithContext(*ctx).Exec(fmt.Sprintf("DELETE FROM %s", pq.QuoteIdentifier(ds.tableName))).Error; err != nil {
		return fmt.Errorf("postgres clear error: %w", err)
	}

	return nil
}

// Close data source
func (ds *PostgresDatasource[T]) Close(ctx *context.Context) error {
	sqlDB, err := ds.db.DB()
	if err != nil {
		return err
	}

	// Stop replication
	if !ds.disableReplication {
		if err := ds.stopReplication(ctx); err != nil {
			slog.Warn("failed to stop replication during close", "error", err)
		}

		// Drop publication if exists
		if !ds.disableReplication {
			sqlDB, err := ds.db.DB()
			if err != nil {
				return err
			}

			// Drop publication if exists
			_, err = sqlDB.Exec(fmt.Sprintf("DROP PUBLICATION IF EXISTS %s", pq.QuoteIdentifier(ds.publicationName)))
			if err != nil {
				return fmt.Errorf("failed to drop publication: %w", err)
			}
		}
	}

	return sqlDB.Close()
}

// -------------------------------------
// Logical Replication using pglogrepl
// -------------------------------------

// getReplicationDSN returns the DSN for replication connection
func (ds *PostgresDatasource[T]) getReplicationDSN() string {
	// Extract DSN from the GORM dialector
	if dialector, ok := ds.db.Dialector.(*postgres.Dialector); ok {
		dsn := dialector.DSN
		// Add replication parameter if not present
		if !strings.Contains(dsn, "replication=") {
			// Parse and reconstruct the DSN to add replication parameter properly
			if strings.Contains(dsn, " ") {
				// Space-separated format: add at the end
				dsn += " replication=database"
			} else {
				// URL format: need to parse properly
				if strings.Contains(dsn, "?") {
					dsn += "&replication=database"
				} else {
					dsn += "?replication=database"
				}
			}
		}
		return dsn
	}
	panic("failed to extract DSN from GORM dialector")
}

// Check PostgreSQL logical replication settings
func (ds *PostgresDatasource[T]) checkLogicalReplication(ctx *context.Context) error {
	sqlDB, err := ds.db.DB()
	if err != nil {
		return err
	}

	// Ensure wal_level is logical
	var walLevel string
	if err := sqlDB.QueryRowContext(*ctx, "SHOW wal_level").Scan(&walLevel); err != nil {
		return fmt.Errorf("failed to check `wal_level`: %w", err)
	}
	if walLevel != "logical" {
		return fmt.Errorf("`wal_level` must be set to 'logical', current: '%s'", walLevel)
	}

	return nil
}

// Create publication for the table
func (ds *PostgresDatasource[T]) createPublication(ctx *context.Context) error {
	sqlDB, err := ds.db.DB()
	if err != nil {
		return err
	}

	// Check if publication exists
	var exists bool
	checkQuery := "SELECT EXISTS(SELECT 1 FROM pg_publication WHERE pubname = $1)"
	if err := sqlDB.QueryRowContext(*ctx, checkQuery, ds.publicationName).Scan(&exists); err != nil {
		return fmt.Errorf("check publication error: %w", err)
	}

	if !exists {
		// Create publication
		createCmd := fmt.Sprintf("CREATE PUBLICATION %s FOR TABLE %s WITH (publish = 'insert,update,delete')",
			pq.QuoteIdentifier(ds.publicationName), pq.QuoteIdentifier(ds.tableName))
		_, err := sqlDB.ExecContext(*ctx, createCmd)
		if err != nil {
			return fmt.Errorf("create publication error: %w", err)
		}
	}

	return nil
}

// ReplicationEvent represents a logical replication event
type ReplicationEventAction string

const (
	ReplicationEventActionInsert ReplicationEventAction = "INSERT"
	ReplicationEventActionUpdate ReplicationEventAction = "UPDATE"
	ReplicationEventActionDelete ReplicationEventAction = "DELETE"
)

type ReplicationEvent struct {
	Action ReplicationEventAction `json:"action"`
	Data   map[string]any         `json:"data"`
}

// parseColumnData converts pglogrepl tuple data to map[string]any
func (ds *PostgresDatasource[T]) parseColumnData(tuple *pglogrepl.TupleData, relation *pglogrepl.RelationMessage) map[string]any {
	if tuple == nil {
		return nil
	}

	result := make(map[string]any)
	for i, col := range tuple.Columns {
		column := relation.Columns[i]
		columnName := column.Name

		switch col.DataType {
		// Handle NULL values
		case pglogrepl.TupleDataTypeNull:
			result[columnName] = nil

		// Handle text and binary data
		case pglogrepl.TupleDataTypeText, pglogrepl.TupleDataTypeBinary:
			// Try to convert to appropriate Go type
			dataStr := string(col.Data)

			// Try boolean first
			if dataStr == "t" || dataStr == "true" {
				result[columnName] = true
			} else if dataStr == "f" || dataStr == "false" {
				result[columnName] = false
			} else if parsedInt, err := strconv.ParseInt(dataStr, 10, 32); err == nil {
				// Try int32
				result[columnName] = int32(parsedInt)
			} else if parsedTime, err := time.Parse(time.RFC3339, dataStr); err == nil {
				// Try time (RFC3339 format)
				result[columnName] = parsedTime
			} else if parsedTime, err := time.Parse(time.RFC3339Nano, dataStr); err == nil {
				// Try time (RFC3339 Nano format)
				result[columnName] = parsedTime
			} else if parsedTime, err := time.Parse("2006-01-02 15:04:05.000000+00", dataStr); err == nil {
				// Try time (PostgreSQL timestamp format)
				result[columnName] = parsedTime
			} else {
				// Try to parse as JSON first
				var jsonData map[string]any
				if err := json.Unmarshal([]byte(dataStr), &jsonData); err == nil {
					result[columnName] = jsonData
				} else {
					// Finally, use as string
					result[columnName] = dataStr
				}
			}
		}
	}

	return result
}

// processLogicalReplicationData processes the logical replication data using pglogrepl
func (ds *PostgresDatasource[T]) processLogicalReplicationData(walData []byte, relations map[uint32]*pglogrepl.RelationMessage) (*ReplicationEvent, error) {
	if len(walData) == 0 {
		return nil, nil
	}

	// Parse the logical replication message using pglogrepl
	logicalMsg, err := pglogrepl.Parse(walData)
	if err != nil {
		return nil, fmt.Errorf("failed to parse logical replication message: %w", err)
	}

	switch msg := logicalMsg.(type) {
	case *pglogrepl.RelationMessage:
		// Cache the relation info for future use (could store column names here)
		relations[msg.RelationID] = msg
		return nil, nil

	case *pglogrepl.InsertMessage:
		// Handle INSERT messages
		data := ds.parseColumnData(msg.Tuple, relations[msg.RelationID])
		return &ReplicationEvent{
			Action: ReplicationEventActionInsert,
			Data:   data,
		}, nil

	case *pglogrepl.UpdateMessage:
		// Handle UPDATE messages - use NewTuple for current values
		data := ds.parseColumnData(msg.NewTuple, relations[msg.RelationID])
		return &ReplicationEvent{
			Action: ReplicationEventActionUpdate,
			Data:   data,
		}, nil

	case *pglogrepl.DeleteMessage:
		// Handle DELETE messages
		var data map[string]any
		if msg.OldTupleType == 'K' || msg.OldTupleType == 'O' {
			data = ds.parseColumnData(msg.OldTuple, relations[msg.RelationID])
		}
		return &ReplicationEvent{
			Action: ReplicationEventActionDelete,
			Data:   data,
		}, nil

	default:
		return nil, nil
	}
}

// startLogicalReplication starts the logical replication using pglogrepl
func (ds *PostgresDatasource[T]) startLogicalReplication(ctx context.Context, watcher chan<- DatasourcePushRequest, processEvent func(event *ReplicationEvent) DatasourcePushRequest) error {
	// Parse DSN to get connection config
	config, err := pgx.ParseConfig(ds.getReplicationDSN())
	if err != nil {
		return fmt.Errorf("postgres replication: failed to parse replication DSN: %w", err)
	}

	// Connect to PostgreSQL with replication protocol
	conn, err := pgconn.Connect(ctx, config.ConnString())
	if err != nil {
		return fmt.Errorf("postgres replication: failed to connect for replication: %w", err)
	}
	defer conn.Close(ctx)

	// Identify system for replication using pglogrepl
	// Used to get the current WAL position for the client's replication slot
	sysIdentity, err := pglogrepl.IdentifySystem(ctx, conn)
	if err != nil {
		return fmt.Errorf("postgres replication: failed to identify system: %w", err)
	}

	slog.Info("Connected to PostgreSQL for logical replication",
		"systemid", sysIdentity.SystemID,
		"timeline", sysIdentity.Timeline,
		"xlogpos", sysIdentity.XLogPos,
		"dbname", sysIdentity.DBName)

	// Start replication from current WAL position
	startLSN := sysIdentity.XLogPos

	// Create replication slot if it doesn't exist
	repSlot, err := pglogrepl.CreateReplicationSlot(ctx, conn, ds.replicationSlotName, "pgoutput",
		pglogrepl.CreateReplicationSlotOptions{Temporary: true, Mode: pglogrepl.LogicalReplication})
	if err != nil {
		// Check if slot already exists (this is expected in most cases)
		if !strings.Contains(err.Error(), "already exists") {
			return fmt.Errorf("failed to create replication slot: %w", err)
		}
		slog.Debug("Replication slot already exists", "slot", ds.replicationSlotName)
	} else {
		if repSlot.ConsistentPoint != "" {
			lsn, err := pglogrepl.ParseLSN(repSlot.ConsistentPoint)
			if err == nil {
				startLSN = lsn
			}
		}
		slog.Info("Created replication slot", "slot", ds.replicationSlotName)
	}

	// Start logical replication from the slot using pglogrepl
	err = pglogrepl.StartReplication(ctx, conn, ds.replicationSlotName, startLSN, pglogrepl.StartReplicationOptions{
		PluginArgs: []string{
			fmt.Sprintf("publication_names '%s'", ds.publicationName),
			"proto_version '1'",
			"messages 'true'",
		},
		Mode: pglogrepl.LogicalReplication,
	})
	if err != nil {
		return fmt.Errorf("postgres replication: failed to start replication: %w", err)
	}

	slog.Info("Started logical replication", "slot", ds.replicationSlotName, "publication", ds.publicationName, "lsn", startLSN)

	// Handle replication messages
	standbyMessageTimeout := time.Second * 10
	nextStandbyMessageDeadline := time.Now().Add(standbyMessageTimeout)
	relations := map[uint32]*pglogrepl.RelationMessage{}

	for {
		select {
		case <-ctx.Done():
			return nil

		default:
			// Send standby status message periodically
			if time.Now().After(nextStandbyMessageDeadline) {
				err = pglogrepl.SendStandbyStatusUpdate(ctx, conn, pglogrepl.StandbyStatusUpdate{
					WALWritePosition: startLSN,
					WALFlushPosition: startLSN,
					WALApplyPosition: startLSN,
					ClientTime:       time.Now(),
					ReplyRequested:   false,
				})
				if err != nil {
					slog.Error("postgres replication: failed to send standby status update", "error", err)
				}
				nextStandbyMessageDeadline = time.Now().Add(standbyMessageTimeout)
			}

			// Skip if busy
			if conn.IsBusy() {
				continue
			}

			// Receive replication message with timeout
			ctxWithTimeout, cancel := context.WithTimeout(ctx, time.Millisecond*500)
			rawMsg, err := conn.ReceiveMessage(ctxWithTimeout)
			cancel()
			if err != nil {
				continue // Proceed to next iteration to retry
			}

			// Process replication messages using pglogrepl
			if copyData, ok := rawMsg.(*pgproto3.CopyData); ok {

				// Determine message type from first byte and parse accordingly
				switch copyData.Data[0] {

				// Primary keepalive message (heartbeat)
				// Used to ensure the standby is still alive
				case pglogrepl.PrimaryKeepaliveMessageByteID:
					pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(copyData.Data[1:])
					if err != nil {
						slog.Error("postgres replication: failed to parse primary keepalive message", "error", err)
						continue
					}

					if pkm.ServerWALEnd > startLSN {
						startLSN = pkm.ServerWALEnd
					}
					if pkm.ReplyRequested {
						err = pglogrepl.SendStandbyStatusUpdate(ctx, conn, pglogrepl.StandbyStatusUpdate{
							WALWritePosition: startLSN,
							WALFlushPosition: startLSN,
							WALApplyPosition: startLSN,
							ClientTime:       time.Now(),
							ReplyRequested:   false,
						})
						if err != nil {
							slog.Error("postgres replication: failed to send standby status update in response to keepalive", "error", err)
						}
						nextStandbyMessageDeadline = time.Now().Add(standbyMessageTimeout)
					}

				// XLogData message containing logical replication data
				// This is where the actual changes are received
				case pglogrepl.XLogDataByteID:
					xld, err := pglogrepl.ParseXLogData(copyData.Data[1:])
					if err != nil {
						slog.Error("postgres replication: failed to parse XLog data", "error", err)
						continue
					}

					startLSN = xld.WALStart + pglogrepl.LSN(len(xld.WALData))

					// Process logical replication message using pglogrepl
					event, err := ds.processLogicalReplicationData(xld.WALData, relations)
					if err != nil {
						slog.Error("postgres replication: failed to process logical replication data", "error", err)
						continue
					}

					// If no event (e.g., relation message), skip
					if event == nil {
						continue
					}

					// Send event to watcher
					watcher <- processEvent(event)

				default:
					slog.Debug("postgres replication: received unknown replication message type")
				}
			} else {
				slog.Debug("postgres replication: received non-CopyData message during replication")
			}
		}
	}
}

// stopReplication stops the logical replication and cleans up resources
func (ds *PostgresDatasource[T]) stopReplication(ctx *context.Context) error {
	// Since we're not using subscriptions with pglogrepl, we just need to clean up the slot
	// The slot will be automatically released when the replication connection closes

	// Connect to PostgreSQL for replication cleanup
	config, err := pgx.ParseConfig(ds.getReplicationDSN())
	if err != nil {
		return fmt.Errorf("failed to parse replication DSN: %w", err)
	}

	// Connect to PostgreSQL with replication protocol
	conn, err := pgconn.Connect(*ctx, config.ConnString())
	if err != nil {
		// If we can't connect, the slot will be cleaned up automatically on server restart
		slog.Warn("failed to connect for replication cleanup", "error", err)
		return nil
	}
	defer conn.Close(*ctx)

	// Drop replication slot using pglogrepl
	err = pglogrepl.DropReplicationSlot(*ctx, conn, ds.replicationSlotName, pglogrepl.DropReplicationSlotOptions{Wait: true})
	if err != nil && !strings.Contains(err.Error(), "does not exist") {
		// Slot might not exist or might be in use - this is not critical
		slog.Warn("failed to drop replication slot during cleanup", "slot", ds.replicationSlotName, "error", err)
	} else {
		slog.Debug("dropped replication slot", "slot", ds.replicationSlotName)
	}

	return nil
}
