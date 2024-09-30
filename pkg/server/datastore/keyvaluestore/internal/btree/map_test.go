package btree

import (
	"fmt"
	"testing"

	"golang.org/x/exp/constraints"
)

func Test(t *testing.T) {
	var byKey Map[KeyValue[string], string, ByKey[string]]
	var byValue Map[KeyValue[string], string, ByValue[string]]

	for _, kv := range []KeyValue[string]{
		{Key: "A", Value: "1"},
		{Key: "B", Value: "4"},
		{Key: "C", Value: "3"},
		{Key: "C", Value: "2"},
		{Key: "C", Value: "1"},
	} {
		byKey.Set(kv, "V:"+kv.Key)
		byValue.Set(kv, "V:"+kv.Key)
	}

	{
		iter := byKey.Iter()
		for ok := iter.First(); ok; ok = iter.Next() {
			fmt.Println("BYKEY:", iter.Key(), iter.Value())
		}
	}
	{
		iter := byValue.Iter()
		for ok := iter.First(); ok; ok = iter.Next() {
			fmt.Println("BYVALUE:", iter.Key(), iter.Value())
		}
	}
}

type KeyValue[V constraints.Ordered] struct {
	Key   string
	Value V
}

type ByKey[V constraints.Ordered] struct{}

func (ByKey[V]) Cmp(a, b KeyValue[V]) int {
	switch {
	case a.Key < b.Key:
		return -1
	case a.Key > b.Key:
		return 1
	}
	switch {
	case a.Value < b.Value:
		return -1
	case a.Value > b.Value:
		return 1
	}
	return 0
}

type ByValue[V constraints.Ordered] struct{}

func (ByValue[V]) Cmp(a, b KeyValue[V]) int {
	switch {
	case a.Value < b.Value:
		return -1
	case a.Value > b.Value:
		return 1
	}
	switch {
	case a.Key < b.Key:
		return -1
	case a.Key > b.Key:
		return 1
	}
	return 0
}
