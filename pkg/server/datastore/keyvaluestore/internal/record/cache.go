package record

import (
	"context"
	"errors"
	"sync"

	"github.com/spiffe/spire/pkg/server/datastore/keyvaluestore/internal/keyvalue"
)

var (
	ErrNotFound = keyvalue.ErrNotFound
	ErrConflict = keyvalue.ErrConflict
	ErrExists   = keyvalue.ErrExists
)

func NewCache[C Codec[O], I Index[O, L], O Object, L any](store keyvalue.Store, kind string, index I) *Cache[C, I, O, L] {
	return &Cache[C, I, O, L]{store: store, kind: kind, index: index}
}

type Cache[C Codec[O], I Index[O, L], O Object, L any] struct {
	kind  string
	codec C

	mtx   sync.RWMutex
	store keyvalue.Store
	index I
}

func (c *Cache[C, I, O, L]) Kind() string {
	return c.kind
}

func (c *Cache[C, I, O, L]) Count() int {
	return c.index.Count()
}

func (c *Cache[C, I, O, L]) ReadIndex(f func(i I)) {
	c.mtx.RLock()
	defer c.mtx.RUnlock()
	f(c.index)
}

func (c *Cache[C, I, O, L]) Get(key string) (*Record[O], error) {
	c.mtx.RLock()
	defer c.mtx.RUnlock()

	r, ok := c.index.Get(key)
	if !ok {
		return nil, ErrNotFound
	}
	return r, nil
}

func (c *Cache[C, I, O, L]) List(opts L) ([]*Record[O], string, error) {
	c.mtx.RLock()
	defer c.mtx.RUnlock()

	iter, err := c.index.List(opts)
	if err != nil {
		return nil, "", err
	}

	var nextCursor string
	var rs []*Record[O]
	for ok := iter.First(); ok; ok = iter.Next() {
		r := iter.Record()
		rs = append(rs, r)
		nextCursor = recordKey(r)
	}

	iter.Release()
	return rs, nextCursor, nil
}

func (c *Cache[C, I, O, L]) Create(ctx context.Context, o O) error {
	key, value, err := c.codec.Marshal(&o)
	if err != nil {
		return err
	}
	if err := c.store.Create(ctx, c.kind, key, value); err != nil {
		return err
	}
	return c.Reload(ctx, key)
}

func (c *Cache[C, I, O, L]) Update(ctx context.Context, o O, revision int64) error {
	key, value, err := c.codec.Marshal(&o)
	if err != nil {
		return err
	}
	if err := c.store.Update(ctx, c.kind, key, value, revision); err != nil {
		return err
	}
	return c.Reload(ctx, key)
}

func (c *Cache[C, I, O, L]) Replace(ctx context.Context, o O) error {
	key, value, err := c.codec.Marshal(&o)
	if err != nil {
		return err
	}
	if err := c.store.Replace(ctx, c.kind, key, value); err != nil {
		return err
	}
	return c.Reload(ctx, key)
}

func (c *Cache[C, I, O, L]) Delete(ctx context.Context, key string) error {
	if err := c.store.Delete(ctx, c.kind, key); err != nil {
		return err
	}
	return c.Reload(ctx, key)
}

func (c *Cache[C, I, O, L]) Reload(ctx context.Context, key string) error {
	c.mtx.Lock()
	defer c.mtx.Unlock()

	r, err := c.get(ctx, key)
	switch {
	case err == nil:
		c.index.Put(r)
		return nil
	case errors.Is(err, keyvalue.ErrNotFound):
		c.index.Delete(key)
		return nil
	default:
		return err
	}
}

func (c *Cache[C, I, O, L]) get(ctx context.Context, key string) (*Record[O], error) {
	kv, err := c.store.Get(ctx, c.kind, key)
	if err != nil {
		return nil, err
	}

	r := &Record[O]{
		Metadata: Metadata(kv.Metadata),
	}
	if err := c.codec.Unmarshal(kv.Value, &r.Object); err != nil {
		return nil, err
	}
	return r, nil
}
