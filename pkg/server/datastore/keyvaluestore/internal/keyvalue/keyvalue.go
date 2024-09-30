package keyvalue

import (
	"context"
	"errors"
	"time"
)

var (
	ErrNotFound = errors.New("record not found")
	ErrConflict = errors.New("record conflict")
	ErrExists   = errors.New("record already exists")
)

type Metadata struct {
	CreatedAt time.Time
	UpdatedAt time.Time
	Revision  int64
}

type Record struct {
	Metadata
	Kind  string
	Key   string
	Value []byte
}

type Store interface {
	Get(ctx context.Context, kind, key string) (Record, error)
	Create(ctx context.Context, kind, key string, value []byte) error
	Update(ctx context.Context, kind, key string, value []byte, revision int64) error
	Replace(ctx context.Context, kind, key string, value []byte) error
	Delete(ctx context.Context, kind, key string) error
	Batch(ctx context.Context, ops []Op) error
	AtomicCounter(ctx context.Context, kind string) (uint, error)
	Watch(ctx context.Context) WatchChan
	Close() error
}

type Op struct {
	Kind     string
	Key      string
	Value    []byte
	Revision int64
	Type     OpType
}

type OpType int

const (
	CreateOp OpType = iota
	UpdateOp
	ReplaceOp
	DeleteOp
)

type WatchChan <-chan WatchResult

type Key struct {
	Kind string
	Key  string
}

type WatchResult struct {
	Current bool
	Changed []Key
	Err     error
}
