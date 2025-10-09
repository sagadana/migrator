package pipelines

import (
	"context"
	"encoding/json"
	"errors"
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

const DefaultFetchBufferSize = 10240

type PipelineConfig struct {
	// Number of items to migrate per batch. 0 to disable batching
	MigrationBatchSize uint64
	// Max amount of items to migrate. 0 to migrate everything
	MigrationMaxSize uint64
	// Table / Collection offset to start reading from
	MigrationStartOffset uint64
	// Number of parallel workers used to read from source
	MigrationParallelWorkers uint

	// Number of items to accumulate & replicate per batch
	ReplicationBatchSize uint64
	// How long to wait for data to be accumulated
	ReplicationBatchWindowSecs uint64

	// Migration Started Callback - Triggered synchronously when migration starts
	OnMigrationStart func(state states.State)
	// Migration Progress Callback - Triggered asynchronously when a batch is migrated
	OnMigrationProgress func(state states.State, count datasources.DatasourcePushCount)
	// Migration Error Callback - Triggered asynchronously when a batch migration fails
	OnMigrationError func(state states.State, data datasources.DatasourcePushRequest, err error)
	// Migration Stopped Callback - Triggered synchronously when migration stops
	OnMigrationStopped func(state states.State)

	// Replication Started Callback - Triggered synchronously when replication starts
	OnReplicationStart func(state states.State)
	// Replication Error Callback - Triggered asynchronously when a batch replication fails
	OnReplicationError func(state states.State, data datasources.DatasourcePushRequest, err error)
	// Replication Progress Callback - Triggered asynchronously when a batch is replicated
	OnReplicationProgress func(state states.State, count datasources.DatasourcePushCount)
	// Replication Stopped Callback - Triggered synchronously when replication stops
	OnReplicationStopped func(state states.State)
}

type Pipeline struct {
	ID        string
	Logger    *slog.Logger
	Store     states.Store
	From      datasources.Datasource
	To        datasources.Datasource
	Transform datasources.DatasourceTransformer
}

type PipelineErrorCode string

const (
	PipelineErrorProcessing PipelineErrorCode = "pipeline_processing"
)

var (
	ErrPipelineMigrating            = errors.New("pipeline is already running migration")
	ErrPipelineReplicating          = errors.New("pipeline is already running replication")
	ErrPipelineMigrationEmptySource = errors.New("pipeline migration stopped due to empty source")
)

// -------------------
// UTILS
// -------------------

func (p *Pipeline) handleExit(ctx *context.Context, state *states.State, config *PipelineConfig, err error) {
	// Migration
	if state.MigrationStatus == states.MigrationStatusStarting ||
		state.MigrationStatus == states.MigrationStatusInProgress {
		state.MigrationStatus = states.MigrationStatusFailed
		state.MigrationStoppedAt = time.Now()
		if r := recover(); r != nil {
			state.MigrationIssue = fmt.Sprintf("Unexpexted Error: %v", r)
		} else if err != nil {
			state.MigrationIssue = err.Error()
		}
		if err := p.SetState(ctx, *state); err != nil {
			panic(err)
		}

		slog.Info("Migration Stopped")
		if config.OnMigrationStopped != nil {
			config.OnMigrationStopped(*state)
		}
	}
	// Replication
	if state.ReplicationStatus == states.ReplicationStatusStarting ||
		state.ReplicationStatus == states.ReplicationStatusStreaming {
		state.ReplicationStatus = states.ReplicationStatusPaused
		state.ReplicationStoppededAt = time.Now()
		if r := recover(); r != nil {
			state.ReplicationIssue = fmt.Sprintf("Unexpexted Error: %v", r)
		} else if err != nil {
			state.MigrationIssue = err.Error()
		}
		if err := p.SetState(ctx, *state); err != nil {
			panic(err)
		}

		slog.Info("Replication Paused")
		if config.OnReplicationStopped != nil {
			config.OnReplicationStopped(*state)
		}
	}
}

func (p *Pipeline) handleUnexpectedExit(ctx *context.Context, state *states.State, config *PipelineConfig) {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM, syscall.SIGABRT, syscall.SIGQUIT)

	select {
	case <-c:
		slog.Info("Unexpected interruption. Updating state...")
		p.handleExit(ctx, state, config, nil)
		os.Exit(0)
	case <-(*ctx).Done():
		slog.Info("Pipeline context closed. Updating state...")
		ctx, cancel := context.WithTimeout(context.Background(), time.Duration(10*time.Second))
		defer cancel()
		p.handleExit(&ctx, state, config, ctx.Err())
		signal.Stop(c)
		close(c)
	}
}

func (p *Pipeline) SetState(ctx *context.Context, state states.State) error {
	return p.Store.Store(ctx, p.ID, state)
}

func (p *Pipeline) GetState(ctx *context.Context) (states.State, bool) {
	return p.Store.Load(ctx, p.ID)
}

// -------------------
// CORE
// -------------------

type CallbackResult = struct {
	Count    *datasources.DatasourcePushCount
	Failures *datasources.DatasourcePushRequest
	Err      error
}

// Process migration
func (p *Pipeline) migrate(
	ctx *context.Context,
	source <-chan datasources.DatasourcePushRequest,
	callback func(result CallbackResult),
) {
	callbackChan := make(chan CallbackResult)

	// Use background worker to trigger callback
	wg := new(sync.WaitGroup)
	wg.Add(1)
	go func(callbackChan <-chan CallbackResult) {
		defer wg.Done()

		var data CallbackResult
		for data = range callbackChan {
			callback(data)
		}
	}(callbackChan)
	defer wg.Wait()
	defer close(callbackChan) // close input chan first before waiting (defer uses LIFO)

	// Reusable vars
	var data datasources.DatasourcePushRequest
	var doc map[string]any
	var transformed map[string]any
	var err error

	for data = range source {

		if p.Transform != nil {

			request := &datasources.DatasourcePushRequest{
				Inserts: make([]map[string]any, 0),
				Updates: make([]map[string]any, 0),
				Deletes: data.Deletes, // Deletes
			}

			// Inserts
			for _, doc = range data.Inserts {
				transformed, err = p.Transform(doc)
				if err != nil {
					callbackChan <- CallbackResult{
						Count: nil,
						Err:   fmt.Errorf("transform error: %w", err),
						Failures: &datasources.DatasourcePushRequest{
							Inserts: []map[string]any{doc},
						},
					}
					continue
				}
				request.Inserts = append(request.Inserts, transformed)
			}

			// Updates
			for _, doc = range data.Updates {
				transformed, err = p.Transform(doc)
				if err != nil {
					callbackChan <- CallbackResult{
						Count: nil,
						Err:   fmt.Errorf("transform error: %w", err),
						Failures: &datasources.DatasourcePushRequest{
							Updates: []map[string]any{doc},
						},
					}
					continue
				}
				request.Updates = append(request.Updates, transformed)
			}

			// Write
			count, err := p.To.Push(ctx, request)
			if err != nil {
				callbackChan <- CallbackResult{
					Count:    nil,
					Err:      fmt.Errorf("migration error: %w", err),
					Failures: request,
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
				callbackChan <- CallbackResult{
					Count:    nil,
					Err:      fmt.Errorf("migration error: %w", err),
					Failures: &data,
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
}

// Process continuous replication
func (p *Pipeline) replicate(
	ctx *context.Context,
	config *PipelineConfig,
	callback func(result CallbackResult),
) {

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
	p.migrate(ctx, input, callback)
}

// -------------------
// PUBLIC
// -------------------

// Watch for changes from source (`From`) and replicate them unto the destination (`To`)
func (p *Pipeline) Stream(ctx *context.Context, config *PipelineConfig) error {

	// Set default logger
	if p.Logger != nil {
		slog.SetDefault(p.Logger)
	}

	// Get current state
	state, ok := p.GetState(ctx)

	// Working? end
	if ok && state.ReplicationStatus == states.ReplicationStatusStarting || state.ReplicationStatus == states.ReplicationStatusStreaming {
		return ErrPipelineReplicating
	}

	// New|Resuming? Mark as starting
	if !ok ||
		state.ReplicationStatus == states.ReplicationStatusPaused {
		state = states.State{
			ReplicationStatus:    states.ReplicationStatusStarting,
			ReplicationStartedAt: time.Now(),
		}
		if err := p.SetState(ctx, state); err != nil {
			return err
		}
	}

	// Graceful shutdown handling
	go p.handleUnexpectedExit(ctx, &state, config)

	// Track state - Streaming
	state.ReplicationStatus = states.ReplicationStatusStreaming
	if err := p.SetState(ctx, state); err != nil {
		return err
	}

	slog.Info("Replication Started")
	if config.OnReplicationStart != nil {
		config.OnReplicationStart(state)
	}

	// Replicate changes
	p.replicate(ctx, config, func(result CallbackResult) {
		if result.Err != nil {
			err := fmt.Errorf("replication pipeline (%s) error: %w", p.ID, result.Err)
			slog.Error(err.Error())

			state.ReplicationIssue = err.Error()
			if config.OnReplicationError != nil {
				defer config.OnReplicationError(state, *result.Failures, err)
			}

			if err = p.SetState(ctx, state); err != nil {
				slog.Error("unexpected replication error", "error", err)
			}
		} else if result.Count != nil && config.OnReplicationProgress != nil {
			config.OnReplicationProgress(state, *result.Count)
		}
	})

	return nil
}

// Execute migration from source (`From`) to destination (`To`)
func (p *Pipeline) Start(ctx *context.Context, config *PipelineConfig, withReplication bool) error {

	// Set default logger
	if p.Logger != nil {
		slog.SetDefault(p.Logger)
	}

	// Get current state
	state, ok := p.GetState(ctx)
	stateMutex := new(sync.RWMutex) // To synchronize concurrent state updates

	var startOffet uint64
	if ok {
		// Use previous offset for resume from last position
		previousOffset, _ := state.MigrationOffset.Int64()
		startOffet = max(uint64(previousOffset), config.MigrationStartOffset)

		// Working? end
		if ok && state.MigrationStatus == states.MigrationStatusStarting || state.MigrationStatus == states.MigrationStatusInProgress {
			return ErrPipelineMigrating
		}
	}

	// Get total items (before saving state)
	total := p.From.Count(ctx, &datasources.DatasourceFetchRequest{
		Size:   config.MigrationMaxSize,
		Offset: startOffet,
	})

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
		if err := p.SetState(ctx, state); err != nil {
			return err
		}
	}

	// Graceful shutdown handling
	go p.handleUnexpectedExit(ctx, &state, config)

	// Listen to change stream for Continuous Replication
	if withReplication {
		wg := new(sync.WaitGroup)
		wg.Add(1)

		// Process continuous replication in the background
		go func() {
			defer wg.Done()
			if err := p.Stream(ctx, config); err != nil {
				panic(err)
			}
		}()

		// Wait for stream to end
		defer wg.Wait()
	} else if total == 0 {

		// Nothing to migrate
		state.MigrationStatus = states.MigrationStatusStopped
		state.MigrationIssue = fmt.Sprintf("pipeline (%s) migration stopped due to empty source", p.ID)
		state.MigrationStoppedAt = time.Now()
		if err := p.SetState(ctx, state); err != nil {
			return err
		}

		return ErrPipelineMigrationEmptySource
	}

	// Track: In Progress
	state.MigrationStatus = states.MigrationStatusInProgress
	state.MigrationOffset = json.Number(strconv.FormatUint(startOffet, 10))
	state.MigrationEstimateCount = json.Number(strconv.FormatUint(total, 10))
	if err := p.SetState(ctx, state); err != nil {
		return err
	}

	slog.Info(fmt.Sprintf("Migration Started. Total: %d, Offset: %d", total, startOffet))
	if config.OnMigrationStart != nil {
		config.OnMigrationStart(state)
	}

	// Process reads in parallel
	sourceBufferSize := max(DefaultFetchBufferSize, int(config.MigrationBatchSize))
	source := make(chan datasources.DatasourceFetchResult, sourceBufferSize)
	parallel := helpers.ParallelBatchConfig{
		Units:       config.MigrationParallelWorkers,
		BatchSize:   config.MigrationBatchSize,
		Total:       total,
		StartOffset: startOffet,
	}
	helpers.ParallelBatch(ctx, &parallel, func(ctx *context.Context, job, size, offset uint64) {
		result := p.From.Fetch(ctx, &datasources.DatasourceFetchRequest{
			Size:   size,
			Offset: offset,
		})
		source <- result
	})

	// Process writes in parallel
	helpers.Parallel(config.MigrationParallelWorkers, func(worker uint) {

		// Parse destination input from source
		input := helpers.StreamTransform(source, func(data datasources.DatasourceFetchResult) datasources.DatasourcePushRequest {
			if data.Err != nil {
				err := fmt.Errorf("migration fetch error (%s): %w", p.ID, data.Err)
				state.MigrationIssue = err.Error()

				if config.OnMigrationError != nil {
					go config.OnMigrationError(
						state,
						datasources.DatasourcePushRequest{Inserts: data.Docs},
						err,
					)
				}

				if err := p.SetState(ctx, state); err != nil {
					slog.Error("unexpected migration error", "error", err)
				}
			}
			return datasources.DatasourcePushRequest{Inserts: data.Docs}
		})

		// Last worker & last job? close source chan
		if worker == config.MigrationParallelWorkers-1 && len(source) <= sourceBufferSize {
			close(source)
		}

		// Send to destination
		p.migrate(ctx, input, func(result CallbackResult) {
			// Lock state for concurrent progress updates
			stateMutex.Lock()
			defer stateMutex.Unlock()

			previousOffset, _ := state.MigrationOffset.Int64()
			previousTotal, _ := state.MigrationTotal.Int64()

			// Track migration progress
			state.MigrationStatus = states.MigrationStatusInProgress
			if result.Err != nil {
				err := fmt.Errorf("migration pipeline (%s) error: %w", p.ID, result.Err)
				slog.Error(err.Error())

				state.MigrationIssue = err.Error()
				if config.OnMigrationError != nil {
					defer config.OnMigrationError(state, *result.Failures, err)
				}
			} else if result.Count != nil {
				state.MigrationOffset = json.Number(strconv.FormatUint(uint64(previousOffset)+result.Count.Inserts, 10))
				state.MigrationTotal = json.Number(strconv.FormatUint(uint64(previousTotal)+result.Count.Inserts, 10))

				slog.Info(fmt.Sprintf("Migration Progress (#%d): %s of %d. Offset: %d - %s", worker, state.MigrationTotal, total, previousOffset, state.MigrationOffset))
				if config.OnMigrationProgress != nil {
					defer config.OnMigrationProgress(state, *result.Count)
				}
			}

			if err := p.SetState(ctx, state); err != nil {
				slog.Error("unexpected migration error", "error", err)
			}
		})

	}).Wait()

	// Track: Success
	state.MigrationStatus = states.MigrationStatusCompleted
	state.MigrationStoppedAt = time.Now()
	if err := p.SetState(ctx, state); err != nil {
		return err
	}

	slog.Info(fmt.Sprintf("Migration Completed. Total: %s, Offset: %s", state.MigrationTotal, state.MigrationOffset))
	if config.OnMigrationStopped != nil {
		config.OnMigrationStopped(state)
	}

	return nil
}
