package datasources

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"reflect"
	"strings"
	"time"

	"github.com/lib/pq"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

const PostgresIDField = "id"
const DefaultPrefix = "migrator_"

type PostgresDatasourceTransformer[T any] func(data map[string]any) (T, error)

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
	Filter map[string]any
	// Sort order (ORDER BY clause)
	Sort map[string]any

	// Provide custom GORM DB instance
	WithDB func(ctx *context.Context, dsn string) (*gorm.DB, error)
	// Provide custom transformer
	WithTransformer PostgresDatasourceTransformer[T]
	// Perform actions on init
	OnInit func(db *gorm.DB) error

	// Logical replication settings
	PublicationName  string
	SubscriptionName string
}

type PostgresDatasource[T any] struct {
	db        *gorm.DB
	tableName string
	model     *T
	idField   string

	filter      map[string]any
	sort        map[string]any
	transformer PostgresDatasourceTransformer[T]

	// Replication settings
	publicationName  string
	subscriptionName string
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
		idField = PostgresIDField
	}

	tableSuffix := strings.ReplaceAll(strings.ToLower(strings.TrimSpace(config.TableName)), "-", "_")

	// Set default replication names
	publicationName := config.PublicationName
	if publicationName == "" {
		publicationName = fmt.Sprintf("%spub_%s", DefaultPrefix, tableSuffix)
	}
	subscriptionName := config.SubscriptionName
	if subscriptionName == "" {
		subscriptionName = fmt.Sprintf("%ssub_%s", DefaultPrefix, tableSuffix)
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

		publicationName:  publicationName,
		subscriptionName: subscriptionName,
	}

	// Auto-migrate the table
	if err := ds.db.Table(config.TableName).AutoMigrate(config.Model); err != nil {
		panic(fmt.Errorf("postgres datasource: failed to auto-migrate table %s: %w", config.TableName, err))
	}

	// Check logical replication
	if err := ds.checkLogicalReplication(ctx); err != nil {
		panic(fmt.Errorf("postgres datasource: %w", err))
	}

	// Create publication
	if err := ds.createPublication(ctx); err != nil {
		panic(fmt.Errorf("postgres datasource: failed to create publication: %w", err))
	}

	// Initialize data source
	if config.OnInit != nil {
		if err := config.OnInit(ds.db); err != nil {
			panic(err)
		}
	}

	return ds
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
		return fmt.Errorf("failed to check publication existence: %w", err)
	}

	if !exists {
		// Create publication
		createCmd := fmt.Sprintf("CREATE PUBLICATION %s FOR TABLE %s WITH (publish = 'insert,update,delete')",
			pq.QuoteIdentifier(ds.publicationName), pq.QuoteIdentifier(ds.tableName))
		_, err := sqlDB.ExecContext(*ctx, createCmd)
		if err != nil {
			return fmt.Errorf("failed to create publication: %w", err)
		}
	}

	return nil
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

// Get total count
func (ds *PostgresDatasource[T]) Count(ctx *context.Context, request *DatasourceFetchRequest) uint64 {
	query := ds.db.Table(ds.tableName)

	// Apply filters
	if len(ds.filter) > 0 {
		for key, value := range ds.filter {
			query = query.Where(fmt.Sprintf("%s = ?", pq.QuoteIdentifier(key)), value)
		}
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
	if len(ds.filter) > 0 {
		for key, value := range ds.filter {
			if strings.Contains(key, "?") {
				query = query.Where(key, value)
			} else {
				query = query.Where(fmt.Sprintf("%s = ?", pq.QuoteIdentifier(key)), value)
			}
		}
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

// Listen to logical replication stream
func (ds *PostgresDatasource[T]) Watch(ctx *context.Context, request *DatasourceStreamRequest) <-chan DatasourceStreamResult {
	watcher := make(chan DatasourcePushRequest)

	// Process PostgreSQL logical replication events
	processEvent := func(action string, data map[string]any) DatasourcePushRequest {
		inserts, updates, deletes := []map[string]any{}, []map[string]any{}, []string{}

		switch strings.ToUpper(action) {
		case "INSERT":
			if data != nil {
				inserts = append(inserts, data)
			}

		case "UPDATE":
			if data != nil {
				updates = append(updates, data)
			}

		case "DELETE":
			if data != nil {
				if id, ok := data[ds.idField]; ok {
					deletes = append(deletes, fmt.Sprintf("%v", id))
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

		// Create subscription for logical replication
		if err := ds.createSubscription(&bgCtx); err != nil {
			slog.Error("postgres watch error", "error", err)
			return
		}

		// Create replication connection
		sqlDB, err := ds.db.DB()
		if err != nil {
			slog.Error("failed to get sql.DB", "error", err)
			return
		}

		// Start replication slot
		tableSuffix := strings.ReplaceAll(strings.ToLower(strings.TrimSpace(ds.tableName)), "-", "_")
		slotName := fmt.Sprintf("%sslot_%s", DefaultPrefix, tableSuffix)
		_, err = sqlDB.ExecContext(bgCtx, fmt.Sprintf("SELECT pg_create_logical_replication_slot('%s', 'pgoutput')", slotName))
		if err != nil {
			// Slot might already exist, try to continue
			slog.Warn("replication slot creation warning", "error", err)
		}

		// Start logical replication stream
		query := fmt.Sprintf("START_REPLICATION SLOT %s LOGICAL 0/0 (proto_version '1', publication_names '%s')",
			slotName, ds.publicationName)

		rows, err := sqlDB.QueryContext(bgCtx, query)
		if err != nil {
			slog.Error("failed to start replication", "error", err)
			return
		}
		defer rows.Close()

		for {
			select {
			case <-bgCtx.Done():
				return
			default:
				if !rows.Next() {
					if err := rows.Err(); err != nil {
						slog.Error("replication stream error", "error", err)
					}
					return
				}

				var lsn, data []byte
				var timestamp time.Time
				if err := rows.Scan(&lsn, &timestamp, &data); err != nil {
					slog.Error("failed to scan replication data", "error", err)
					continue
				}

				// Parse logical replication message
				event, err := ds.parseLogicalReplicationMessage(data)
				if err != nil {
					slog.Error("failed to parse replication message", "error", err)
					continue
				}

				if event == nil {
					continue
				}

				// Apply filters if configured
				if len(ds.filter) > 0 && event.Data != nil {
					match := true
					for key, expectedValue := range ds.filter {
						if actualValue, exists := event.Data[key]; !exists || !reflect.DeepEqual(actualValue, expectedValue) {
							match = false
							break
						}
					}
					if !match {
						continue
					}
				}

				watcher <- processEvent(event.Action, event.Data)
			}
		}
	}(*ctx)

	return StreamChanges(
		ctx,
		fmt.Sprintf("postgres table '%s'", ds.tableName),
		watcher,
		request,
	)
}

// Create subscription for logical replication
func (ds *PostgresDatasource[T]) createSubscription(ctx *context.Context) error {
	sqlDB, err := ds.db.DB()
	if err != nil {
		return err
	}

	// Check if subscription exists
	var exists bool
	checkQuery := "SELECT EXISTS(SELECT 1 FROM pg_subscription WHERE subname = $1)"
	if err := sqlDB.QueryRowContext(*ctx, checkQuery, ds.subscriptionName).Scan(&exists); err != nil {
		return fmt.Errorf("failed to check subscription existence: %w", err)
	}

	if !exists {
		// Create subscription
		createCmd := fmt.Sprintf("CREATE SUBSCRIPTION %s CONNECTION '%s' PUBLICATION %s WITH (copy_data = false)",
			pq.QuoteIdentifier(ds.subscriptionName),
			ds.db.Dialector.(*postgres.Dialector).Config.DSN,
			pq.QuoteIdentifier(ds.publicationName))

		_, err := sqlDB.ExecContext(*ctx, createCmd)
		if err != nil {
			return fmt.Errorf("failed to create subscription: %w", err)
		}
	}

	return nil
}

// ReplicationEvent represents a logical replication event
type ReplicationEvent struct {
	Action string         `json:"action"`
	Data   map[string]any `json:"data"`
}

// Parse logical replication message (simplified implementation)
func (ds *PostgresDatasource[T]) parseLogicalReplicationMessage(data []byte) (*ReplicationEvent, error) {
	if len(data) == 0 {
		return nil, nil
	}

	// This is a simplified parser for logical replication messages
	// In a real implementation, you would need to properly parse the pgoutput format
	// For demonstration purposes, we'll assume the data is JSON formatted
	var event ReplicationEvent
	if err := json.Unmarshal(data, &event); err != nil {
		// If not JSON, try to parse as simple format
		dataStr := string(data)
		if strings.HasPrefix(dataStr, "INSERT:") {
			event.Action = "INSERT"
			// Parse insert data...
		} else if strings.HasPrefix(dataStr, "UPDATE:") {
			event.Action = "UPDATE"
			// Parse update data...
		} else if strings.HasPrefix(dataStr, "DELETE:") {
			event.Action = "DELETE"
			// Parse delete data...
		} else {
			return nil, fmt.Errorf("unknown message format: %s", dataStr)
		}
	}

	return &event, nil
}

// Clear data source
func (ds *PostgresDatasource[T]) Clear(ctx *context.Context) error {
	return ds.db.WithContext(*ctx).Exec(fmt.Sprintf("DELETE FROM %s", pq.QuoteIdentifier(ds.tableName))).Error
}

// Close data source
func (ds *PostgresDatasource[T]) Close(ctx *context.Context) error {
	sqlDB, err := ds.db.DB()
	if err != nil {
		return err
	}
	return sqlDB.Close()
}
