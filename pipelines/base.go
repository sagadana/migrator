package pipelines

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/sagadana/migrator/datasources"
	"github.com/sagadana/migrator/helpers"
	"github.com/sagadana/migrator/states"
)

type PipelineConfig struct {
	MigrationBatchSize    int64
	MigrationMaxSize      int64
	MigrationStartOffset  int64
	MigrationParallelLoad int

	ContinuousReplication      bool
	ReplicationBatchSize       int64
	ReplicationBatchWindowSecs int64

	OnMigrationStart      func(state states.State)
	OnMigrationProgress   func(state states.State, count datasources.DatasourcePushCount)
	OnMigrationError      func(state states.State, err error)
	OnMigrationStopped    func(state states.State)
	OnReplicationStart    func(state states.State)
	OnReplicationError    func(state states.State, err error)
	OnReplicationProgress func(state states.State, count datasources.DatasourcePushCount)
	OnReplicationStopped  func(state states.State)
}

type Pipeline struct {
	ID     string
	Logger *slog.Logger
	Store  states.Store
	From   datasources.Datasource
	To     datasources.Datasource
	// Use this the convert the JSON data to a different JSON formatted output
	//
	// TODO: For other datasources like redis that wouldnlt always use JSON,
	// use a dedicated Struct to manage the transformation back and forth.
	// E.g: ```go
	// type RedisTransformer struct {
	// 	strings    map[string]string
	// 	lists      [][]string
	// 	sets       map[string][]string
	// 	hashes     map[string]map[string]string
	// 	sortedSets map[string][]map[string]float64
	// }
	// ````
	Transform datasources.DatasourceTransformer
}

type PipelineErrorCode string

const (
	PipelineErrorProcessing  PipelineErrorCode = "pipeline_processing"
	PipelineErrorSourceEmpty PipelineErrorCode = "pipeline_source_empty"
)

type PipelineError struct {
	Code    PipelineErrorCode
	Message string
}

func (e *PipelineError) Error() string {
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

// -------------------
// UTILS
// -------------------

func (p *Pipeline) handleExit(ctx *context.Context) {
	state, ok := p.GetState(ctx)
	if ok {
		// Migration
		if state.MigrationStatus == states.MigrationStatusStarting ||
			state.MigrationStatus == states.MigrationStatusInProgress {
			state.MigrationStatus = states.MigrationStatusFailed
			state.MigrationStoppedAt = time.Now()
			if r := recover(); r != nil {
				state.MigrationIssue = fmt.Sprintf("Unexpexted Error: %v", r)
			}
			p.setState(ctx, state)
		}
		// Replication
		if state.ReplicationStatus == states.ReplicationStatusStarting ||
			state.ReplicationStatus == states.ReplicationStatusStreaming {
			state.ReplicationStatus = states.ReplicationStatusPaused
			state.ReplicationStoppededAt = time.Now()
			if r := recover(); r != nil {
				state.ReplicationIssue = fmt.Sprintf("Unexpexted Error: %v", r)
			}
			p.setState(ctx, state)
		}
	}
}

func (p *Pipeline) handleUnexpectedExit(ctx *context.Context) {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	<-c

	slog.Info("Unexpected interruption. Updating state...")
	p.handleExit(ctx)

	close(c)
	os.Exit(0)

}

func (p *Pipeline) setState(ctx *context.Context, state states.State) error {
	return p.Store.Store(ctx, p.ID, state)
}

func (p *Pipeline) GetState(ctx *context.Context) (states.State, bool) {
	return p.Store.Load(ctx, p.ID)
}

// -------------------
// CORE
// -------------------

type CallbackResult = struct {
	Count *datasources.DatasourcePushCount
	Err   error
}

// Process migration
func (p *Pipeline) migrate(
	ctx *context.Context,
	source <-chan datasources.DatasourcePushRequest,
	callback func(result CallbackResult),
) error {
	callbackChan := make(chan CallbackResult)

	var lastError error

	// Use background worker to trigger callback
	wg := new(sync.WaitGroup)
	wg.Add(1)
	go func(callbackChan <-chan CallbackResult) {
		defer wg.Done()
		for data := range callbackChan {
			callback(data)
		}
	}(callbackChan)
	defer wg.Wait()
	defer close(callbackChan) // close input chan first before waiting (defer = LIFO)

	for data := range source {

		if p.Transform != nil {

			// Reusable vars
			var transformed map[string]any
			var err error

			request := &datasources.DatasourcePushRequest{
				Updates: make(map[string]map[string]any),
				Inserts: make([]map[string]any, 0),
				Deletes: data.Deletes, // Deletes
			}

			// Inserts
			for _, doc := range data.Inserts {
				transformed, err = p.Transform(doc)
				if err != nil {
					lastError = fmt.Errorf("transform error: %w", err)
					callbackChan <- CallbackResult{
						Count: nil,
						Err:   lastError,
					}
					continue
				}
				request.Inserts = append(request.Inserts, transformed)
			}

			// Updates
			for id, doc := range data.Updates {
				transformed, err = p.Transform(doc)
				if err != nil {
					lastError = fmt.Errorf("transform error: %w", err)
					callbackChan <- CallbackResult{
						Count: nil,
						Err:   lastError,
					}
					continue
				}
				request.Updates[id] = transformed
			}

			// Write
			count, err := p.To.Push(ctx, request)
			if err != nil {
				lastError = fmt.Errorf("migration error: %w", err)
				callbackChan <- CallbackResult{
					Count: nil,
					Err:   lastError,
				}
				continue
			}

			// Notify complete
			if count.Inserts+count.Updates+count.Deletes > 0 {
				callbackChan <- CallbackResult{
					Count: &count,
					Err:   nil,
				}
			}
		} else {

			// Write
			count, err := p.To.Push(ctx, &data)
			if err != nil {
				lastError = fmt.Errorf("migration error: %w", err)
				callbackChan <- CallbackResult{
					Count: nil,
					Err:   lastError,
				}
				continue
			}

			// Notify complete
			if count.Inserts+count.Updates+count.Deletes > 0 {
				callbackChan <- CallbackResult{
					Count: &count,
					Err:   nil,
				}
			}
		}
	}

	return lastError
}

// Process continuous replication
func (p *Pipeline) replicate(
	ctx *context.Context,
	config *PipelineConfig,
	callback func(result CallbackResult),
) error {

	// Watch for changes
	watcher := p.From.Watch(ctx, &datasources.DatasourceStreamRequest{
		BatchSize:          config.ReplicationBatchSize,
		BatchWindowSeconds: config.ReplicationBatchWindowSecs,
	})

	// Parse destination input from stream
	input := helpers.StreamTransform(watcher, func(data datasources.DatasourceStreamResult) datasources.DatasourcePushRequest {
		if data.Err != nil {
			defer callback(CallbackResult{
				Count: nil,
				Err:   data.Err,
			})
		}
		return data.Docs
	})

	// Send to destination
	return p.migrate(ctx, input, callback)
}

// -------------------
// PUBLIC
// -------------------

// Watch for changes from source (`From`) and replicate them unto the destination (`To`)
func (p *Pipeline) Stream(ctx *context.Context, config PipelineConfig) error {

	// Set default logger
	if p.Logger != nil {
		slog.SetDefault(p.Logger)
	}

	// Get current state
	state, ok := p.GetState(ctx)

	// New|Resuming? Mark as starting
	if !ok ||
		state.ReplicationStatus == states.ReplicationStatusPaused ||
		state.ReplicationStatus == states.ReplicationStatusFailed {
		state = states.State{
			ReplicationStatus:    states.ReplicationStatusStarting,
			ReplicationStartedAt: time.Now(),
		}
		p.setState(ctx, state)
	}

	// Working? end
	if state.ReplicationStatus == states.ReplicationStatusStreaming {
		return &PipelineError{
			Code:    PipelineErrorProcessing,
			Message: fmt.Sprintf("pipeline (%s) is already streaming. StartedAt: %s", p.ID, state.ReplicationStartedAt.String()),
		}
	}

	// Graceful shutdown handling
	go p.handleUnexpectedExit(ctx)
	defer p.handleExit(ctx)

	// Track state - Streaming
	state.ReplicationStatus = states.ReplicationStatusStreaming
	p.setState(ctx, state)

	slog.Info("Replication Started")
	if config.OnReplicationStart != nil {
		go config.OnReplicationStart(state)
	}

	// Replicate changes
	err := p.replicate(ctx, &config, func(result CallbackResult) {
		if result.Err != nil && config.OnReplicationError != nil {
			config.OnReplicationError(state, result.Err)
		} else if result.Count != nil && config.OnReplicationProgress != nil {
			config.OnReplicationProgress(state, *result.Count)
		}
	})

	// Track: Failed
	if err != nil {
		state.ReplicationStatus = states.ReplicationStatusFailed
		state.ReplicationIssue = err.Error()
		state.ReplicationStoppededAt = time.Now()
		p.setState(ctx, state)

		slog.Error(fmt.Sprintf("Replication Failed: %s", state.ReplicationIssue))
		if config.OnReplicationStopped != nil {
			go config.OnReplicationStopped(state)
		}

		return err
	}

	// Track: Paused
	state.ReplicationStatus = states.ReplicationStatusPaused
	state.ReplicationStoppededAt = time.Now()
	p.setState(ctx, state)

	slog.Info("Replication Paused")
	if config.OnReplicationStopped != nil {
		go config.OnReplicationStopped(state)
	}

	return nil
}

// Execute migration from source (`From`) to destination (`To`)
func (p *Pipeline) Start(ctx *context.Context, config PipelineConfig) error {

	// Set default logger
	if p.Logger != nil {
		slog.SetDefault(p.Logger)
	}

	// Get current state
	state, ok := p.GetState(ctx)
	var stateMutex sync.RWMutex // To synchronize concurrent state updates

	// New|Resuming? Mark as starting
	if !ok ||
		state.MigrationStatus == states.MigrationStatusCompleted ||
		state.MigrationStatus == states.MigrationStatusFailed {
		state = states.State{
			MigrationStatus:    states.MigrationStatusStarting,
			MigrationOffset:    json.Number(strconv.FormatInt(0, 10)),
			MigrationTotal:     json.Number(strconv.FormatInt(0, 10)),
			MigrationStartedAt: time.Now(),
		}
		p.setState(ctx, state)
	}

	// Working? end
	if state.MigrationStatus == states.MigrationStatusInProgress {
		return &PipelineError{
			Code:    PipelineErrorProcessing,
			Message: fmt.Sprintf("pipeline (%s) is already migrating. StartedAt: %s", p.ID, state.ReplicationStartedAt.String()),
		}
	}

	// Graceful shutdown handling
	go p.handleUnexpectedExit(ctx)
	defer p.handleExit(ctx)

	// Use previous offset for resume from last position
	previousOffset, _ := state.MigrationOffset.Int64()
	startOffet := max(previousOffset, config.MigrationStartOffset)

	// Get total items
	total := p.From.Count(ctx, &datasources.DatasourceFetchRequest{
		Size:   config.MigrationMaxSize,
		Offset: startOffet,
	})

	parallel := helpers.ParallelConfig{
		Units:       config.MigrationParallelLoad,
		BatchSize:   config.MigrationBatchSize,
		Total:       total,
		StartOffset: startOffet,
	}

	// Listen to change stream for Continuous Replication
	if config.ContinuousReplication {
		wg := new(sync.WaitGroup)
		wg.Add(1)

		// Process continuous replication in the background
		go func(ctx *context.Context, p *Pipeline, config *PipelineConfig) {
			defer wg.Done()
			p.Stream(ctx, *config)
		}(ctx, p, &config)

		// Wait forever
		defer wg.Wait()
	} else if total == 0 {

		msg := fmt.Sprintf("pipeline (%s) migration stopped due to empty source", p.ID)

		// Nothing to migrate
		state.MigrationStatus = states.MigrationStatusStopped
		state.MigrationIssue = msg
		state.MigrationStoppedAt = time.Now()
		p.setState(ctx, state)

		return &PipelineError{
			Code:    PipelineErrorProcessing,
			Message: msg,
		}
	}

	// Track: In Progress
	state.MigrationStatus = states.MigrationStatusInProgress
	state.MigrationOffset = json.Number(strconv.FormatInt(startOffet, 10))
	p.setState(ctx, state)

	slog.Info(fmt.Sprintf("Migration Started. Total: %d, Offset: %d", total, startOffet))
	if config.OnMigrationStart != nil {
		go config.OnMigrationStart(state)
	}

	// Fetch from source
	source := helpers.ParallelRead(ctx, &parallel,
		func(ctx *context.Context, job int, size, offset int64) datasources.DatasourceFetchResult {
			result := p.From.Fetch(ctx, &datasources.DatasourceFetchRequest{
				Size:   size,
				Offset: offset,
			})
			return result
		})

	// Parse destination input from source
	input := helpers.StreamTransform(source, func(data datasources.DatasourceFetchResult) datasources.DatasourcePushRequest {
		if data.Err != nil {
			if config.OnMigrationError != nil {
				go config.OnMigrationError(state, fmt.Errorf("fetch error (%s): %w", p.ID, data.Err))
			}
		}
		return datasources.DatasourcePushRequest{Inserts: data.Docs}
	})

	// Send to destination
	err := p.migrate(ctx, input, func(result CallbackResult) {
		// Lock state for concurrent progress updates
		stateMutex.Lock()
		defer stateMutex.Unlock()

		previousOffset, _ := state.MigrationOffset.Int64()
		previousTotal, _ := state.MigrationTotal.Int64()

		// Track migration progress
		state.MigrationStatus = states.MigrationStatusInProgress
		if result.Err != nil {
			state.MigrationIssue = result.Err.Error()

			slog.Error(fmt.Sprintf("Migration Error: %s", result.Err.Error()))
			if config.OnMigrationError != nil {
				defer config.OnMigrationError(state, result.Err)
			}
		} else if result.Count != nil {
			state.MigrationOffset = json.Number(strconv.FormatInt(previousOffset+result.Count.Inserts, 10))
			state.MigrationTotal = json.Number(strconv.FormatInt(previousTotal+result.Count.Inserts, 10))

			slog.Info(fmt.Sprintf("Migration Progress: %s of %d. Offset: %d - %s", state.MigrationTotal, total, previousOffset, state.MigrationOffset))
			if config.OnMigrationProgress != nil {
				defer config.OnMigrationProgress(state, *result.Count)
			}
		}

		p.setState(ctx, state)
	})

	// Track: Failed
	if err != nil {
		state.MigrationStatus = states.MigrationStatusFailed
		state.MigrationIssue = err.Error()
		state.MigrationStoppedAt = time.Now()
		p.setState(ctx, state)

		slog.Error(fmt.Sprintf("Migration Failed: %s", state.MigrationIssue))
		if config.OnMigrationStopped != nil {
			go config.OnMigrationStopped(state)
		}

		return err
	}

	// Track: Success
	state.MigrationStatus = states.MigrationStatusCompleted
	state.MigrationStoppedAt = time.Now()
	p.setState(ctx, state)

	slog.Info(fmt.Sprintf("Migration Completed. Total: %s, Offset: %s", state.MigrationTotal, state.MigrationOffset))
	if config.OnMigrationStopped != nil {
		go config.OnMigrationStopped(state)
	}

	return nil
}
