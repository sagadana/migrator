package pipelines

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sagadana/migrator/datasources"
	"sagadana/migrator/states"
	"strconv"
	"syscall"
	"time"
)

type PipelineConfig struct {
	ParrallelLoad              int
	BatchSize                  int64
	MaxSize                    int64
	StartOffset                int64
	ContinuousReplication      bool
	ReplicationBatchSize       int64
	ReplicationBatchWindowSecs int64
}

type Pipeline[F datasources.Datasource, T datasources.Datasource] struct {
	ID        string
	Store     states.Store
	From      F
	To        T
	Transform func(data *any) (*any, error)
}

// -------------------
// UTILS
// -------------------

func (p *Pipeline[F, T]) getState(ctx *context.Context) (states.State, bool) {
	return p.Store.Load(ctx, p.ID)
}

func (p *Pipeline[F, T]) setState(ctx *context.Context, state states.State) error {
	return p.Store.Store(ctx, p.ID, state)
}

func (p *Pipeline[F, T]) handleExit(ctx *context.Context) {
	state, ok := p.getState(ctx)
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
			state.ReplicationStopedAt = time.Now()
			if r := recover(); r != nil {
				state.ReplicationIssue = fmt.Sprintf("Unexpexted Error: %v", r)
			}
			p.setState(ctx, state)
		}
	}
}

func (p *Pipeline[F, T]) handleUnexpectedExit(ctx *context.Context) {
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

// Process migration
func (p *Pipeline[F, T]) migrate(
	ctx *context.Context,
	parallel *ParallelConfig,
	source <-chan datasources.DatasourcePushRequest,
) (int64, error) {
	return ParallelWrite(ctx, parallel, source,
		func(ctx *context.Context, data datasources.DatasourcePushRequest) (int64, error) {

			var newCount int64

			// Transform
			if p.Transform != nil {
				request := datasources.DatasourcePushRequest{
					Inserts: []any{},
					Updates: map[string]any{},
					Deletes: data.Deletes,
				}
				// Inserts
				for _, doc := range data.Inserts {
					transformed, err := p.Transform(&doc)
					if err != nil {
						return 0, err
					}
					request.Inserts = append(request.Inserts, transformed)
				}
				// Updates
				for id, doc := range data.Updates {
					transformed, err := p.Transform(&doc)
					if err != nil {
						return 0, err
					}
					request.Updates[id] = transformed
				}

				// Write
				if err := p.To.Push(ctx, &request); err != nil {
					return 0, err
				}

				newCount = int64(len(request.Inserts))
			} else {
				// Write
				if err := p.To.Push(ctx, &data); err != nil {
					return newCount, err
				}
				newCount = int64(len(data.Inserts))
			}

			return newCount, nil
		})
}

// Process continuous replication
func (p *Pipeline[F, T]) replicate(
	ctx *context.Context,
	config *PipelineConfig,
	parallel *ParallelConfig,
) error {

	// Watch for changes
	watcher := p.From.Watch(ctx, &datasources.DatasourceStreamRequest{
		BatchSize:          config.ReplicationBatchSize,
		BatchWindowSeconds: config.ReplicationBatchWindowSecs,
	})

	// Parse destination input from stream
	input := make(chan datasources.DatasourcePushRequest)
	go func() {
		defer close(input)
		for stream := range watcher {
			input <- stream.Docs
		}
	}()

	// Send to destination
	_, err := p.migrate(ctx, parallel, input)
	return err
}

// -------------------
// PUBLIC
// -------------------

// Watch for changes from source (`From`) and replicate them unto the destination (`To`)
func (p *Pipeline[F, T]) Stream(ctx *context.Context, config PipelineConfig) error {

	parallel := ParallelConfig{
		Units:     config.ParrallelLoad,
		BatchSize: config.BatchSize,
	}

	// Initialize data sources
	p.From.Init()
	p.To.Init()

	// Get current state
	state, ok := p.getState(ctx)

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

	// Replicate changes
	err := p.replicate(ctx, &config, &parallel)
	if err != nil {
		// Track state - Failed
		state.ReplicationStatus = states.ReplicationStatusFailed
		state.ReplicationIssue = err.Error()
		state.ReplicationStopedAt = time.Now()
		p.setState(ctx, state)
		return err
	}

	// Track state - Paused
	state.ReplicationStatus = states.ReplicationStatusPaused
	state.ReplicationStopedAt = time.Now()
	p.setState(ctx, state)
	return nil
}

// Execute migration from source (`From`) to destination (`To`)
func (p *Pipeline[F, T]) Start(ctx *context.Context, config PipelineConfig) error {

	// Initialize data sources
	p.From.Init()
	p.To.Init()

	// Get current state
	state, ok := p.getState(ctx)

	// New|Resuming? Mark as starting
	if !ok ||
		state.MigrationStatus == states.MigrationStatusCompleted ||
		state.MigrationStatus == states.MigrationStatusFailed {
		state = states.State{
			MigrationStatus:    states.MigrationStatusStarting,
			MigrationOffset:    json.Number(strconv.FormatInt(config.StartOffset, 10)),
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

	// Get previous values
	previousOffset, _ := state.MigrationOffset.Int64()
	previousTotal, _ := state.MigrationTotal.Int64()

	// Use previous offset for resume from last position
	startOffet := max(previousOffset, config.StartOffset)
	log.Printf("Migration - Start offset: %d", startOffet)

	// Get total items
	total := p.From.Count(ctx, &datasources.DatasourceFetchRequest{
		Size: config.MaxSize,
		Skip: startOffet,
	})
	log.Printf("Migration - Total: %d", total)

	parallel := ParallelConfig{
		Units:       config.ParrallelLoad,
		Total:       total,
		BatchSize:   config.BatchSize,
		StartOffset: startOffet,
	}

	if config.ContinuousReplication {
		// Listen to change stream
		go p.replicate(ctx, &config, &parallel)
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

	// Fetch from source
	source := ParallelRead(ctx, &parallel,
		func(ctx *context.Context, job int, size, skip int64) datasources.DatasourceFetchResult {
			log.Printf("Base : ParallelRead: size: %d, skip: %d", size, skip)
			return p.From.Fetch(ctx, &datasources.DatasourceFetchRequest{
				Size: size,
				Skip: skip,
			})
		})

	// Parse destination input from source
	input := make(chan datasources.DatasourcePushRequest)
	go func() {
		defer close(input)
		for data := range source {
			input <- datasources.DatasourcePushRequest{
				Inserts: data.Docs,
			}
		}
	}()

	// Send to destination
	count, err := p.migrate(ctx, &parallel, input)

	// Track state - If continuous replication is disabled
	if !config.ContinuousReplication {
		// Failed
		if err != nil {
			state.MigrationStatus = states.MigrationStatusFailed
			state.MigrationIssue = err.Error()
			state.MigrationTotal = json.Number(strconv.FormatInt(previousTotal+count, 10))
			state.MigrationOffset = json.Number(strconv.FormatInt(previousOffset+count, 10))
			state.MigrationStopedAt = time.Now()
			p.setState(ctx, state)
			return err
		}
		// Success
		state.MigrationStatus = states.MigrationStatusCompleted
		state.MigrationTotal = json.Number(strconv.FormatInt(previousTotal+count, 10))
		state.MigrationOffset = json.Number(strconv.FormatInt(previousOffset+count, 10))
		state.MigrationStopedAt = time.Now()
		p.setState(ctx, state)
	}

	return nil
}
