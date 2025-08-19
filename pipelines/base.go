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
	ParallelLoad               int
	BatchSize                  int64
	MaxSize                    int64
	StartOffset                int64
	ContinuousReplication      bool
	ReplicationBatchSize       int64
	ReplicationBatchWindowSecs int64

	OnMigrationStart      func()
	OnMigrationProgress   func(count MigrateCount)
	OnMigrationComplete   func(state states.State)
	OnReplicationStart    func()
	OnReplicationProgress func(count MigrateCount)
	OnReplicationPause    func(state states.State)
}

type Pipeline struct {
	ID        string
	Store     states.Store
	From      datasources.Datasource
	To        datasources.Datasource
	Transform func(data map[string]any) (map[string]any, error)
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

func (p *Pipeline) GetState(ctx *context.Context) (states.State, bool) {
	return p.Store.Load(ctx, p.ID)
}

func (p *Pipeline) setState(ctx *context.Context, state states.State) error {
	return p.Store.Store(ctx, p.ID, state)
}

func (p *Pipeline) handleExit(ctx *context.Context) {
	state, ok := p.GetState(ctx)
	if ok {
		// Migration
		if state.MigrationStatus == states.MigrationStatusStarting ||
			state.MigrationStatus == states.MigrationStatusInProgress {
			state.MigrationStatus = states.MigrationStatusFailed
			state.MigrationStopedAt = time.Now()
			if r := recover(); r != nil {
				state.MigrationIssue = fmt.Sprintf("Unexpexted Error: %v", r)
			}
			p.setState(ctx, state)
		}
		// Replication
		if state.ReplicationStatus == states.ReplicationStatusStarting ||
			state.ReplicationStatus == states.ReplicationStatusStreaming {
			state.ReplicationStatus = states.ReplicationStatusPaused
			state.ReplicatiOnMigrationCompleteedAt = time.Now()
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

// -------------------
// CORE
// -------------------

type MigrateCount struct {
	inserts int64
	updates int64
	deletes int64
}

// Process migration
func (p *Pipeline) migrate(
	ctx *context.Context,
	source <-chan datasources.DatasourcePushRequest,
	track func(count MigrateCount),
) error {

	for data := range source {

		count := MigrateCount{}

		if p.Transform != nil {

			request := datasources.DatasourcePushRequest{
				Inserts: []map[string]any{},
				Updates: map[string]map[string]any{},
				Deletes: data.Deletes,
			}

			// Inserts
			for _, doc := range data.Inserts {
				transformed, err := p.Transform(doc)
				if err != nil {
					return err
				}
				request.Inserts = append(request.Inserts, transformed)
			}
			// Updates
			for id, doc := range data.Updates {
				transformed, err := p.Transform(doc)
				if err != nil {
					return err
				}
				request.Updates[id] = transformed
			}

			// Write
			if err := p.To.Push(ctx, &request); err != nil {
				return err
			}

			count.inserts = int64(len(request.Inserts))
			count.updates = int64(len(request.Updates))
			count.deletes = int64(len(request.Deletes))
		} else {
			// Write
			if err := p.To.Push(ctx, &data); err != nil {
				return err
			}

			count.inserts = int64(len(data.Inserts))
			count.updates = int64(len(data.Updates))
			count.deletes = int64(len(data.Deletes))
		}

		// Track progress
		track(count)
	}

	return nil
}

// Process continuous replication
func (p *Pipeline) replicate(
	ctx *context.Context,
	config *PipelineConfig,
) error {

	// Watch for changes
	watcher := p.From.Watch(ctx, &datasources.DatasourceStreamRequest{
		BatchSize:          config.ReplicationBatchSize,
		BatchWindowSeconds: config.ReplicationBatchWindowSecs,
	})

	// Parse destination input from stream
	input := helpers.StreamTransform(watcher, func(data datasources.DatasourceStreamResult) datasources.DatasourcePushRequest {
		return data.Docs
	})

	// Send to destination
	return p.migrate(ctx, input, func(count MigrateCount) {
		if config.OnReplicationProgress != nil {
			go config.OnReplicationProgress(count)
		}
	})
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
			ReplicationStatus:             states.ReplicationStatusStarting,
			ReplicatiOnMigrationStartedAt: time.Now(),
		}
		p.setState(ctx, state)
	}

	// Working? end
	if state.ReplicationStatus == states.ReplicationStatusStreaming {
		return &PipelineError{
			Code:    PipelineErrorProcessing,
			Message: fmt.Sprintf("pipeline (%s) is already streaming. StartedAt: %s", p.ID, state.ReplicatiOnMigrationStartedAt.String()),
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
	err := p.replicate(ctx, &config)

	// Track: Failed
	if err != nil {
		state.ReplicationStatus = states.ReplicationStatusFailed
		state.ReplicationIssue = err.Error()
		state.ReplicatiOnMigrationCompleteedAt = time.Now()
		p.setState(ctx, state)
		return err
	}

	// Track: Paused
	state.ReplicationStatus = states.ReplicationStatusPaused
	state.ReplicatiOnMigrationCompleteedAt = time.Now()
	p.setState(ctx, state)

	log.Printf("Replication Paused")
	if config.OnReplicationPause != nil {
		go config.OnReplicationPause(state)
	}

	return nil
}

// Execute migration from source (`From`) to destination (`To`)
func (p *Pipeline) Start(ctx *context.Context, config PipelineConfig) error {

	// Get current state
	state, ok := p.GetState(ctx)

	// New|Resuming? Mark as starting
	if !ok ||
		state.MigrationStatus == states.MigrationStatusCompleted ||
		state.MigrationStatus == states.MigrationStatusFailed {
		state = states.State{
			MigrationStatus:             states.MigrationStatusStarting,
			MigrationOffset:             json.Number(strconv.FormatInt(config.StartOffset, 10)),
			MigratiOnMigrationStartedAt: time.Now(),
		}
		p.setState(ctx, state)
	}

	// Working? end
	if state.MigrationStatus == states.MigrationStatusInProgress {
		return &PipelineError{
			Code:    PipelineErrorProcessing,
			Message: fmt.Sprintf("pipeline (%s) is already migrating. StartedAt: %s", p.ID, state.ReplicatiOnMigrationStartedAt.String()),
		}
	}

	// Graceful shutdown handling
	go p.handleUnexpectedExit(ctx)
	defer p.handleExit(ctx)

	// Use previous offset for resume from last position
	previousOffset, _ := state.MigrationOffset.Int64()
	startOffet := max(previousOffset, config.StartOffset)

	// Get total items
	total := p.From.Count(ctx, &datasources.DatasourceFetchRequest{
		Size:   config.MaxSize,
		Offset: startOffet,
	})

	parallel := helpers.ParallelConfig{
		Units:       config.ParallelLoad,
		Total:       total,
		BatchSize:   config.BatchSize,
		StartOffset: startOffet,
	}

	// Listen to change stream for Continuous Replication
	if config.ContinuousReplication {
		wg := new(sync.WaitGroup)
		wg.Add(1)
		go func() {
			defer wg.Done()

			// Track state - Streaming
			state.ReplicationStatus = states.ReplicationStatusStreaming
			p.setState(ctx, state)

			log.Printf("Replication Started")
			if config.OnReplicationStart != nil {
				go config.OnReplicationStart()
			}

			// Replicate changes
			err := p.replicate(ctx, &config)

			// Track: Failed
			if err != nil {
				state.ReplicationStatus = states.ReplicationStatusFailed
				state.ReplicationIssue = err.Error()
				state.ReplicatiOnMigrationCompleteedAt = time.Now()
				p.setState(ctx, state)
				return
			}

			// Track: Paused
			state.ReplicationStatus = states.ReplicationStatusPaused
			state.ReplicatiOnMigrationCompleteedAt = time.Now()
			p.setState(ctx, state)

			log.Printf("Replication Paused")
			if config.OnReplicationPause != nil {
				go config.OnReplicationPause(state)
			}
		}()

		// Wait for Continuous Replication to end
		defer wg.Wait()
	}

	// Nothing to migrate
	if total == 0 {
		return &PipelineError{
			Code:    PipelineErrorProcessing,
			Message: fmt.Sprintf("pipeline (%s) migration stopped due to empty source", p.ID),
		}
	}

	// Track state - If continuous replication is disabled
	if !config.ContinuousReplication {
		// In Progress
		state.MigrationStatus = states.MigrationStatusInProgress
		p.setState(ctx, state)
	}

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
	err := p.migrate(ctx, input, func(count MigrateCount) {
		// Track migration progress
		previousOffset, _ := state.MigrationOffset.Int64()
		previousTotal, _ := state.MigrationTotal.Int64()
		state.MigrationStatus = states.MigrationStatusInProgress
		state.MigrationTotal = json.Number(strconv.FormatInt(previousTotal+count.inserts, 10))
		state.MigrationOffset = json.Number(strconv.FormatInt(previousOffset+count.inserts, 10))
		p.setState(ctx, state)

		log.Printf("Migration Progress: %s of %d. Offset: %d - %s", state.MigrationTotal, total, previousOffset, state.MigrationOffset)
		if config.OnMigrationProgress != nil {
			go config.OnMigrationProgress(count)
		}
	})

	// Track: Failed
	if err != nil {
		state.MigrationStatus = states.MigrationStatusFailed
		state.MigrationIssue = err.Error()
		state.MigrationStopedAt = time.Now()
		p.setState(ctx, state)
		return err
	}

	// Track: Success
	state.MigrationStatus = states.MigrationStatusCompleted
	state.MigrationStopedAt = time.Now()
	p.setState(ctx, state)

	log.Printf("Migration Completed. Total: %s, Offset: %s", state.MigrationTotal, state.MigrationOffset)
	if config.OnMigrationComplete != nil {
		go config.OnMigrationComplete(state)
	}

	return nil
}
