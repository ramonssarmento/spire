package record

import (
	"github.com/spiffe/spire/pkg/server/datastore/keyvaluestore/internal/btree"
)

type Set[O Object] struct {
	m btree.Map[string, *Record[O], btree.String]
}

func (s *Set[O]) Count() int {
	return s.m.Count()
}

func (s *Set[O]) Get(key string) (*Record[O], bool) {
	return s.m.Get(key)
}

func (s *Set[O]) Delete(key string) {
	s.m.Delete(key)
}

func (s *Set[O]) Set(r *Record[O]) {
	s.m.Set(r.Object.Key(), r)
}

func (s *Set[O]) Iterate(cursor string) Iterator[O] {
	return &SetIter[O]{iter: s.m.Iter(), cursorKey: makeCursorKey(cursor)}
}

type SetIter[O any] struct {
	iter      btree.MapIter[string, *Record[O], btree.String]
	cursorKey string
}

func (it *SetIter[O]) First() bool {
	if it.cursorKey != "" {
		return it.iter.Seek(it.cursorKey)
	}
	return it.iter.First()
}

func (it *SetIter[O]) Next() bool {
	return it.iter.Next()
}

func (it *SetIter[O]) Record() *Record[O] {
	return it.iter.Value()
}

func (it *SetIter[O]) Release() {
}
