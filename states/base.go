package states

import (
	"context"
	"encoding/json"
	"time"
)

type MigrationStatusType string
type ReplicationStatusType string

const (
	MigrationStatusStarting   MigrationStatusType = "starting"
	MigrationStatusInProgress MigrationStatusType = "in_progress"
	MigrationStatusCompleted  MigrationStatusType = "completed"
	MigrationStatusFailed     MigrationStatusType = "failed"
)
const (
	ReplicationStatusStarting  ReplicationStatusType = "starting"
	ReplicationStatusPaused    ReplicationStatusType = "paused"
	ReplicationStatusFailed    ReplicationStatusType = "failed"
	ReplicationStatusStreaming ReplicationStatusType = "streaming"
)

type State struct {
	MigrationStatus    MigrationStatusType `json:"migration_status"`
	MigrationTotal     json.Number         `json:"migration_total"`
	MigrationOffset    json.Number         `json:"migration_offset"`
	MigrationIssue     string              `json:"migration_issue"`
	MigrationStartedAt time.Time           `json:"migration_started_at"`
	MigrationStopedAt  time.Time           `json:"migration_stoped_at"`

	ReplicationStatus    ReplicationStatusType `json:"replication_status"`
	ReplicationIssue     string                `json:"replication_issue"`
	ReplicationStartedAt time.Time             `json:"replication_started_at"`
	ReplicationStopedAt  time.Time             `json:"replication_stoped_at"`
}

type Store interface {
	Store(ctx *context.Context, key string, state State) error
	Load(ctx *context.Context, key string) (State, bool)
	Delete(ctx *context.Context, key string) error
	Clear(ctx *context.Context) error
	Close(ctx *context.Context) error
}
