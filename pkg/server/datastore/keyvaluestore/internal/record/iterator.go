package record

import (
	"sort"

	"github.com/spiffe/spire/pkg/server/datastore/keyvaluestore/internal/btree"
)

func And[O Object](its []Iterator[O]) Iterator[O] {
	switch len(its) {
	case 0:
		return emptyIter[O]{}
	case 1:
		return its[0]
	}
	return &andIter[O]{its: iterators[O](its)}
}

type andIter[O Object] struct {
	its  iterators[O]
	base int
}

func (and *andIter[O]) Record() *Record[O] {
	if and.base == 0 {
		return and.its[0].Record()
	}
	return nil
}

func (and *andIter[O]) First() bool {
	and.base = and.its.first()
	ok := and.match()
	return ok
}

func (and *andIter[O]) Next() bool {
	if and.base >= len(and.its) {
		return false
	}
	and.base = and.its.step()
	return and.match()
}

func (and *andIter[O]) Release() {
	and.its.Release()
}

func (and *andIter[O]) match() bool {
	if and.base != 0 {
		return false
	}
	for i := len(and.its) - 1; i > 0; i-- {
		for recordKey(and.its[i-1].Record()) < recordKey(and.its[i].Record()) {
			if !and.its[i-1].Next() {
				and.base = len(and.its)
				return false
			}
		}
		sort.Sort(and.its)
	}
	return true
}

func Or[O Object](its []Iterator[O]) Iterator[O] {
	switch len(its) {
	case 0:
		return emptyIter[O]{}
	case 1:
		return its[0]
	}
	return &orIter[O]{its: iterators[O](its)}
}

type orIter[O Object] struct {
	its  iterators[O]
	base int
}

func (or *orIter[O]) Record() *Record[O] {
	if or.base < len(or.its) {
		return or.its[or.base].Record()
	}
	return nil
}

func (or *orIter[O]) First() bool {
	or.base = or.its.first()
	return or.base < len(or.its)
}

func (or *orIter[O]) Next() bool {
	if or.base >= len(or.its) {
		return false
	}

	lastKey := recordKey(or.its[or.base].Record())
	if !or.its[or.base].Next() {
		or.base++
	}
	for i := or.base; i < len(or.its); {
		sort.Sort(or.its[or.base:])
		next := or.its[or.base].Record()
		if recordKey(next) != lastKey {
			return true
		}
		if !or.its[or.base].Next() {
			or.base++
			i++
		}
	}
	return false
}

func (or *orIter[O]) Release() {
	or.its.Release()
}

type iterators[O Object] []Iterator[O]

func (its iterators[O]) Len() int {
	return len(its)
}

func (its iterators[O]) Less(a, b int) bool {
	return recordKey(its[a].Record()) < recordKey(its[b].Record())
}

func (its iterators[O]) Swap(a, b int) {
	its[a], its[b] = its[b], its[a]
}

func (its iterators[O]) Release() {
	for _, it := range its {
		it.Release()
	}
}

func (its iterators[O]) first() int {
	exhausted := 0
	for _, it := range its {
		if !it.First() {
			exhausted++
		}
	}
	sort.Sort(its)
	return exhausted
}

func (its iterators[O]) step() int {
	exhausted := 0
	for _, it := range its {
		if !it.Next() {
			exhausted++
		}
	}
	sort.Sort(its)
	return exhausted
}

type exceptIter[O Object, F any, C Cmp[F]] struct {
	iter   Iterator[O]
	byKey  *btree.Map[keyValue[F], *Record[O], byKey[F, C]]
	fields []F
	cmp    func(F, F) int
}

func (it *exceptIter[O, F, C]) Record() *Record[O] {
	return it.iter.Record()
}

func (it *exceptIter[O, F, C]) First() bool {
	for ok := it.iter.First(); ok; ok = it.iter.Next() {
		if it.passes() {
			return true
		}
	}
	return false
}

func (it *exceptIter[O, F, C]) Next() bool {
	for ok := it.iter.Next(); ok; ok = it.iter.Next() {
		if it.passes() {
			return true
		}
	}
	return false
}

func (it *exceptIter[O, F, C]) Release() {
	it.iter.Release()
}

func (it *exceptIter[O, F, C]) passes() bool {
	r := it.Record()
	except := it.byKey.Iter()
	for ok := except.Seek(keyValue[F]{Key: recordKey(r)}); ok; ok = except.Next() {
		kv := except.Key()
		if kv.Key != recordKey(r) {
			break
		}
		if !fieldInSet[F, C](it.fields, kv.Value) {
			return false
		}
	}
	return true
}

func makeCursorKey(cursor string) string {
	if cursor != "" {
		return cursor + string(rune(0))
	}
	return ""
}

type byValueIter[C Cmp[F], O any, F any] struct {
	iter      btree.MapIter[keyValue[F], *Record[O], byValue[F, C]]
	cursorKey string
	field     F
}

func (it *byValueIter[C, O, F]) Record() *Record[O] {
	return it.iter.Value()
}

func (it *byValueIter[C, O, F]) First() bool {
	ok := it.iter.Seek(keyValue[F]{Key: it.cursorKey, Value: it.field})
	return ok && it.ok()
}

func (it *byValueIter[C, O, F]) Next() bool {
	ok := it.iter.Next()
	return ok && it.ok()
}

func (it *byValueIter[C, O, F]) ok() bool {
	return (C{}).Cmp(it.iter.Key().Value, it.field) == 0
}

func (it *byValueIter[C, O, F]) Release() {
}

type byKeyIter[C Cmp[F], O any, F any] struct {
	iter      btree.MapIter[keyValue[F], *Record[O], byKey[F, C]]
	cursorKey string
	field     F
}

func (it *byKeyIter[C, O, F]) Record() *Record[O] {
	return it.iter.Value()
}

func (it *byKeyIter[C, O, F]) First() bool {
	ok := it.iter.Seek(keyValue[F]{Key: it.cursorKey, Value: it.field})
	return ok && it.ok()
}

func (it *byKeyIter[C, O, F]) Next() bool {
	ok := it.iter.Next()
	return ok && it.ok()
}

func (it *byKeyIter[C, O, F]) ok() bool {
	return (C{}).Cmp(it.iter.Key().Value, it.field) == 0
}

func (it *byKeyIter[C, O, F]) Release() {
}

func fieldInSet[F any, C Cmp[F]](fields []F, field F) bool {
	var cmp C
	n := sort.Search(len(fields), func(i int) bool {
		return cmp.Cmp(fields[i], field) >= 0
	})
	return n < len(fields) && cmp.Cmp(fields[n], field) == 0
}

type emptyIter[O any] struct{}

func (emptyIter[O]) First() bool        { return false }
func (emptyIter[O]) Next() bool         { return false }
func (emptyIter[O]) Record() *Record[O] { return nil }
func (emptyIter[O]) Release()           {}

func Limit[O Object](iter Iterator[O], limit int) Iterator[O] {
	return &limitIter[O]{iter: iter, limit: limit}
}

type limitIter[O Object] struct {
	iter  Iterator[O]
	limit int
	count int
}

func (it *limitIter[O]) Record() *Record[O] {
	return it.iter.Record()
}

func (it *limitIter[O]) First() bool {
	it.count = 0
	return it.iter.First() && it.count < it.limit
}

func (it *limitIter[O]) Next() bool {
	if it.count+1 < it.limit {
		it.count++
		return it.iter.Next()
	}
	return false
}

func (it *limitIter[O]) Release() {
	it.iter.Release()
}
