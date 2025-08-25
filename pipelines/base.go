package pipelines

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
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

	OnMigrationStart      func()
	OnMigrationProgress   func(count MigrateCount)
	OnMigrationError      func(err error)
	OnMigrationStopped    func(state states.State)
	OnReplicationStart    func()
	OnReplicationError    func(err error)
	OnReplicationProgress func(count MigrateCount)
	OnReplicationStopped  func(state states.State)
}

type Pipeline struct {
	ID    string
	Store states.Store
	From  datasources.Datasource
	To    datasources.Datasource
	// Use this the convert the JSON data to a different JSON formatted output
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

	log.Printf("Unexpected interruption. Updating state...")
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

type MigrateCount struct {
	inserts int64
	updates int64
	deletes int64
}

type CallbackResult = struct {
	count *MigrateCount
	err   error
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

		count := new(MigrateCount)

		if p.Transform != nil {

			request := new(datasources.DatasourcePushRequest)

			// Reusable vars
			var transformed map[string]any
			var err error

			// Inserts
			for _, doc := range data.Inserts {
				transformed, err = p.Transform(doc)
				if err != nil {
					lastError = fmt.Errorf("transform error: %w", err)
					callbackChan <- CallbackResult{
						count: nil,
						err:   lastError,
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
						count: nil,
						err:   lastError,
					}
					continue
				}
				request.Updates[id] = transformed
			}

			// Write
			if err = p.To.Push(ctx, request); err != nil {
				lastError = fmt.Errorf("migration error: %w", err)
				callbackChan <- CallbackResult{
					count: nil,
					err:   lastError,
				}
				continue
			}

			count.inserts = int64(len(request.Inserts))
			count.updates = int64(len(request.Updates))
			count.deletes = int64(len(request.Deletes))
		} else {
			// Write
			if err := p.To.Push(ctx, &data); err != nil {
				lastError = fmt.Errorf("migration error: %w", err)
				callbackChan <- CallbackResult{
					count: nil,
					err:   lastError,
				}
				continue
			}

			count.inserts = int64(len(data.Inserts))
			count.updates = int64(len(data.Updates))
			count.deletes = int64(len(data.Deletes))
		}

		// Notify complete
		callbackChan <- CallbackResult{
			count: count,
			err:   nil,
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
			callback(CallbackResult{
				count: nil,
				err:   data.Err,
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

	log.Printf("Replication Started")
	if config.OnReplicationStart != nil {
		go config.OnReplicationStart()
	}

	// Replicate changes
	err := p.replicate(ctx, &config, func(result CallbackResult) {
		if result.err != nil && config.OnReplicationError != nil {
			config.OnReplicationError(result.err)
		} else if result.count != nil && config.OnReplicationProgress != nil {
			config.OnReplicationProgress(*result.count)
		}
	})

	// Track: Failed
	if err != nil {
		state.ReplicationStatus = states.ReplicationStatusFailed
		state.ReplicationIssue = err.Error()
		state.ReplicationStoppededAt = time.Now()
		p.setState(ctx, state)

		log.Printf("Replication Failed: %s", state.ReplicationIssue)
		if config.OnReplicationStopped != nil {
			go config.OnReplicationStopped(state)
		}

		return err
	}

	// Track: Paused
	state.ReplicationStatus = states.ReplicationStatusPaused
	state.ReplicationStoppededAt = time.Now()
	p.setState(ctx, state)

	log.Printf("Replication Paused")
	if config.OnReplicationStopped != nil {
		go config.OnReplicationStopped(state)
	}

	return nil
}

// Execute migration from source (`From`) to destination (`To`)
func (p *Pipeline) Start(ctx *context.Context, config PipelineConfig) error {

	// Get current state
	state, ok := p.GetState(ctx)
	var stateMutex sync.RWMutex // To synchronize concurrent state updates

	// New|Resuming? Mark as starting
	if !ok ||
		state.MigrationStatus == states.MigrationStatusCompleted ||
		state.MigrationStatus == states.MigrationStatusFailed {
		state = states.State{
			MigrationStatus:    states.MigrationStatusStarting,
			MigrationOffset:    json.Number(strconv.FormatInt(config.MigrationStartOffset, 10)),
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
		Total:       total,
		BatchSize:   config.MigrationBatchSize,
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
		// Nothing to migrate
		return &PipelineError{
			Code:    PipelineErrorProcessing,
			Message: fmt.Sprintf("pipeline (%s) migration stopped due to empty source", p.ID),
		}
	}

	// Track: In Progress
	state.MigrationStatus = states.MigrationStatusInProgress
	state.MigrationOffset = json.Number(strconv.FormatInt(startOffet, 10))
	state.MigrationTotal = json.Number(strconv.FormatInt(0, 10))
	p.setState(ctx, state)

	log.Printf("Migration Started. Total: %d, Offset: %d", total, startOffet)
	if config.OnMigrationStart != nil {
		go config.OnMigrationStart()
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
		return datasources.DatasourcePushRequest{Inserts: data.Docs}
	})

	// Send to destination
	err := p.migrate(ctx, input, func(result CallbackResult) {
		// Lock state for simultaneous progress updates
		stateMutex.Lock()
		defer stateMutex.Unlock()

		previousOffset, _ := state.MigrationOffset.Int64()
		previousTotal, _ := state.MigrationTotal.Int64()

		// Track migration progress
		state.MigrationStatus = states.MigrationStatusInProgress
		if result.err != nil {
			state.MigrationIssue = result.err.Error()

			log.Printf("Migration Error: %s", result.err.Error())
			if config.OnMigrationError != nil {
				config.OnMigrationError(result.err)
			}
		} else if result.count != nil {
			state.MigrationOffset = json.Number(strconv.FormatInt(previousOffset+result.count.inserts, 10))
			state.MigrationTotal = json.Number(strconv.FormatInt(previousTotal+result.count.inserts, 10))

			log.Printf("Migration Progress: %s of %d. Offset: %d - %s", state.MigrationTotal, total, previousOffset, state.MigrationOffset)
			if config.OnMigrationProgress != nil {
				config.OnMigrationProgress(*result.count)
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

		log.Printf("Migration Failed: %s", state.MigrationIssue)
		if config.OnMigrationStopped != nil {
			go config.OnMigrationStopped(state)
		}

		return err
	}

	// Track: Success
	state.MigrationStatus = states.MigrationStatusCompleted
	state.MigrationStoppedAt = time.Now()
	p.setState(ctx, state)

	log.Printf("Migration Completed. Total: %s, Offset: %s", state.MigrationTotal, state.MigrationOffset)
	if config.OnMigrationStopped != nil {
		go config.OnMigrationStopped(state)
	}

	return nil
}
