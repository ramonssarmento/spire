package record

import (
	"context"
	"io"
	"sync"

	"github.com/spiffe/spire/pkg/server/datastore/keyvaluestore/internal/keyvalue"
)

type Sink interface {
	Kind() string
	Reload(ctx context.Context, key string) error
}

func Watch(ctx context.Context, store keyvalue.Store, sinks []Sink) (io.Closer, error) {
	ch := store.Watch(ctx)

	w := &watcher{
		ch:    ch,
		sinks: make(map[string][]Sink),
	}

	for _, sink := range sinks {
		w.sinks[sink.Kind()] = append(w.sinks[sink.Kind()], sink)
	}

	// wait until the initial load is complete
	if err := w.watch(ctx, func(r keyvalue.WatchResult) bool {
		return r.Current
	}); err != nil {
		return nil, err
	}

	// now watch until closed
	watchCtx, watchCancel := context.WithCancel(context.Background())
	w.cancel = watchCancel
	w.wg.Add(1)
	go func() {
		defer w.wg.Done()
		w.watch(watchCtx, nil)
	}()
	return w, nil
}

type watcher struct {
	sinks  map[string][]Sink
	ch     keyvalue.WatchChan
	wg     sync.WaitGroup
	cancel context.CancelFunc
}

func (w *watcher) Close() error {
	w.cancel()
	w.wg.Wait()
	return nil
}

func (w *watcher) watch(ctx context.Context, until func(r keyvalue.WatchResult) bool) error {
	for {
		select {
		case r := <-w.ch:
			if r.Err != nil {
				return r.Err
			}
			for _, changed := range r.Changed {
				for _, sink := range w.sinks[changed.Kind] {
					if err := sink.Reload(ctx, changed.Key); err != nil {
						return err
					}
				}
			}
			if until != nil && until(r) {
				return nil
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
