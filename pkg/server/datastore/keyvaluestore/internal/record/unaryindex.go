package record

import (
	"github.com/spiffe/spire/pkg/server/datastore/keyvaluestore/internal/btree"
	"golang.org/x/exp/constraints"
)

type UnaryIndex[O Object, F constraints.Ordered] struct {
	UnaryIndexCmp[O, F, OrderedCmp[F]]
}

type UnaryIndexCmp[O Object, F any, C Cmp[F]] struct {
	byValue btree.Map[keyValue[F], *Record[O], byValue[F, C]]
}

func (idx *UnaryIndexCmp[O, F, C]) Set(r *Record[O], field F) {
	key := recordKey(r)
	idx.drop(key)
	idx.byValue.Set(keyValue[F]{Key: key, Value: field}, r)
}

func (idx *UnaryIndexCmp[O, F, C]) Delete(key string) {
	idx.drop(key)
}

func (idx *UnaryIndexCmp[O, F, C]) LessThan(cursor string, pivot F) Iterator[O] {
	// LessThan queries can't be lazily evaluated since the index is sorted by
	// field/object and naive iteration will traverse many field values which
	// will produce multiple sets of objects in ascending order. Lazy iteration
	// requires all results to be evaluated in ascending order. Therefore, less
	// queries gather all results and return an iterator into an intermediate
	// set of sorted objects.
	var results Set[O]

	cursorKey := makeCursorKey(cursor)

	// Do smart seeks to honor the cursor and skip irrelevant objects for each
	// field value encountered.
	var cmp C
	it := idx.byValue.Iter()
	for ok := it.First(); ok; {
		kv := it.Key()
		if cmp.Cmp(kv.Value, pivot) >= 0 {
			break
		}
		if kv.Key <= cursorKey {
			// This object should be skipped by the cursor. Seek past the
			// cursor for this field value.
			ok = it.Seek(keyValue[F]{Key: cursorKey, Value: kv.Value})
			continue
		}
		results.Set(it.Value())
		ok = it.Next()
	}

	return results.Iterate("")
}

func (idx *UnaryIndexCmp[O, F, C]) GreaterThan(cursor string, pivot F) Iterator[O] {
	// GreaterThan queries can't be lazily evaluated since the index is sorted by
	// field/object and naive iteration will traverse many field values which
	// will produce multiple sets of objects in ascending order. Lazy iteration
	// requires all results to be evaluated in ascending order. Therefore, less
	// queries gather all results and return an iterator into an intermediate
	// set of sorted objects.
	var results Set[O]

	cursorKey := makeCursorKey(cursor)

	// Do smart seeks to honor the cursor and skip irrelevant objects for each
	// field value encountered.
	var cmp C
	it := idx.byValue.Iter()
	for ok := it.First(); ok; {
		kv := it.Key()
		if cmp.Cmp(kv.Value, pivot) <= 0 {
			break
		}
		if kv.Key <= cursorKey {
			// This object should be skipped by the cursor. Seek past the
			// cursor for this field value.
			ok = it.Seek(keyValue[F]{Key: cursorKey, Value: kv.Value})
			continue
		}
		results.Set(it.Value())
		ok = it.Next()
	}

	return results.Iterate("")
}

func (idx *UnaryIndexCmp[O, F, C]) EqualTo(cursor string, field F) Iterator[O] {
	return &byValueIter[C, O, F]{iter: idx.byValue.Iter(), cursorKey: makeCursorKey(cursor), field: field}
}

func (idx *UnaryIndexCmp[O, F, C]) drop(key string) {
	iter := idx.byValue.Iter()
	if ok := iter.Seek(keyValue[F]{Key: key}); ok && iter.Key().Key == key {
		idx.byValue.Delete(iter.Key())
	}
}
