package record

import (
	"sort"

	"github.com/spiffe/spire/pkg/server/datastore/keyvaluestore/internal/btree"
	"golang.org/x/exp/constraints"
)

type MatchBehavior int

const (
	MatchAny MatchBehavior = iota + 1
	MatchExact
	MatchSuperset
	MatchSubset
)

type MultiIndex[O Object, F constraints.Ordered] struct {
	MultiIndexCmp[O, F, OrderedCmp[F]]
}

type MultiIndexCmp[O Object, F any, C Cmp[F]] struct {
	byValue btree.Map[keyValue[F], *Record[O], byValue[F, C]]
	byKey   btree.Map[keyValue[F], *Record[O], byKey[F, C]]
}

func (idx *MultiIndexCmp[O, F, C]) Set(r *Record[O], fields []F) {
	key := recordKey(r)
	idx.drop(key)
	for _, field := range fields {
		idx.byKey.Set(keyValue[F]{Key: key, Value: field}, r)
		idx.byValue.Set(keyValue[F]{Key: key, Value: field}, r)
	}
}

func (idx *MultiIndexCmp[O, F, C]) Delete(key string) {
	idx.drop(key)
}

func (idx *MultiIndexCmp[O, F, C]) Matching(cursor string, fields []F, match MatchBehavior) Iterator[O] {
	switch match {
	case MatchAny:
		return Or[O](idx.makeFieldIterators(cursor, fields))
	case MatchExact:
		return &exceptIter[O, F, C]{
			iter:   And[O](idx.makeFieldIterators(cursor, fields)),
			byKey:  &idx.byKey,
			fields: sortFields[C](fields),
		}
	case MatchSubset:
		return &exceptIter[O, F, C]{
			iter:   Or[O](idx.makeFieldIterators(cursor, fields)),
			byKey:  &idx.byKey,
			fields: sortFields[C](fields),
		}
	case MatchSuperset:
		return And[O](idx.makeFieldIterators(cursor, fields))
	}
	// Unexpected
	return emptyIter[O]{}
}

func (idx *MultiIndexCmp[O, F, C]) makeFieldIterators(cursor string, fields []F) []Iterator[O] {
	cursorKey := makeCursorKey(cursor)
	its := make([]Iterator[O], 0, len(fields))
	for _, field := range fields {
		its = append(its, &byValueIter[C, O, F]{iter: idx.byValue.Iter(), cursorKey: cursorKey, field: field})
	}
	return its
}

func (idx *MultiIndexCmp[O, F, C]) drop(key string) {
	var kvs []keyValue[F]
	iter := idx.byKey.Iter()
	for ok := iter.Seek(keyValue[F]{Key: key}); ok; ok = iter.Next() {
		iterKey := iter.Key()
		if iterKey.Key != key {
			break
		}
		kvs = append(kvs, keyValue[F]{Key: iterKey.Key, Value: iterKey.Value})
	}
	for _, kv := range kvs {
		idx.byValue.Delete(keyValue[F](kv))
		idx.byKey.Delete(keyValue[F](kv))
	}
}

func sortFields[C Cmp[F], F any](fields []F) []F {
	var cmp C
	sort.Slice(fields, func(a, b int) bool {
		return cmp.Cmp(fields[a], fields[b]) < 0
	})
	return fields
}
