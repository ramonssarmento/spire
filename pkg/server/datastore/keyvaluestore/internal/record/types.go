package record

import "time"

type Object interface {
	Key() string
}

type Metadata struct {
	CreatedAt time.Time
	UpdatedAt time.Time
	Revision  int64
}

type Record[O any] struct {
	Metadata Metadata
	Object   O
}

type Index[O Object, L any] interface {
	Get(key string) (*Record[O], bool)
	Put(*Record[O]) error
	Delete(key string)
	List(L) (Iterator[O], error)
	Count() int
}

type Iterator[O any] interface {
	First() bool
	Next() bool
	Record() *Record[O]
	Release()
}

type Codec[O any] interface {
	Marshal(o *O) (string, []byte, error)
	Unmarshal(in []byte, out *O) error
}
