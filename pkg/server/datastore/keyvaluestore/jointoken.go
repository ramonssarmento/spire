package keyvaluestore

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/spiffe/spire/pkg/server/datastore"
	"github.com/spiffe/spire/pkg/server/datastore/keyvaluestore/internal/record"
)

func (ds *DataStore) CreateJoinToken(ctx context.Context, token *datastore.JoinToken) error {
	if token == nil || token.Token == "" || token.Expiry.IsZero() {
		return errors.New("token and expiry are required")
	}

	if err := ds.joinTokens.Create(ctx, joinTokenObject{JoinToken: token}); err != nil {
		return dsErr(err, "failed to create entry")
	}
	return nil
}

func (ds *DataStore) DeleteJoinToken(ctx context.Context, token string) error {
	return ds.joinTokens.Delete(ctx, token)
}

func (ds *DataStore) FetchJoinToken(ctx context.Context, token string) (*datastore.JoinToken, error) {
	out, err := ds.joinTokens.Get(token)
	switch {
	case err == nil:
		return out.Object.JoinToken, nil
	case errors.Is(err, record.ErrNotFound):
		return nil, nil
	default:
		return nil, dsErr(err, "failed to fetch join token relationship")
	}
}

func (ds *DataStore) PruneJoinTokens(ctx context.Context, expiresBefore time.Time) error {
	records, _, err := ds.joinTokens.List(&listJoinTokens{
		ByExpiresBefore: expiresBefore,
	})
	if err != nil {
		return err
	}

	var errCount int
	var firstErr error
	for _, record := range records {
		if err := ds.joinTokens.Delete(ctx, record.Object.JoinToken.Token); err != nil {
			if firstErr == nil {
				firstErr = err
			}
			errCount++
		}
	}

	if firstErr != nil {
		return dsErr(firstErr, "failed pruning %d of %d entries: first error", errCount, len(records))
	}
	return nil
}

type joinTokenObject struct {
	JoinToken *datastore.JoinToken
}

func (o joinTokenObject) Key() string {
	if o.JoinToken == nil {
		return ""
	}
	return o.JoinToken.Token
}

type joinTokenCodec struct{}

func (joinTokenCodec) Marshal(o *joinTokenObject) (string, []byte, error) {
	data, err := json.Marshal(o.JoinToken)
	if err != nil {
		return "", nil, err
	}
	return o.Key(), data, nil
}

func (joinTokenCodec) Unmarshal(in []byte, out *joinTokenObject) error {
	joinToken := new(datastore.JoinToken)
	if err := json.Unmarshal(in, joinToken); err != nil {
		return err
	}
	out.JoinToken = joinToken
	return nil
}

type listJoinTokens struct {
	ByExpiresBefore time.Time
}

type joinTokenIndex struct {
	all       record.Set[joinTokenObject]
	expiresAt record.UnaryIndex[joinTokenObject, int64]
}

func (c *joinTokenIndex) Count() int {
	return c.all.Count()
}

func (c *joinTokenIndex) Get(key string) (*record.Record[joinTokenObject], bool) {
	return c.all.Get(key)
}

func (c *joinTokenIndex) Put(r *record.Record[joinTokenObject]) error {
	c.all.Set(r)
	c.expiresAt.Set(r, r.Object.JoinToken.Expiry.Unix())
	return nil
}

func (c *joinTokenIndex) Delete(key string) {
	c.all.Delete(key)
	c.expiresAt.Delete(key)
}

func (c *joinTokenIndex) List(opts *listJoinTokens) (record.Iterator[joinTokenObject], error) {
	var filters []record.Iterator[joinTokenObject]
	if !opts.ByExpiresBefore.IsZero() {
		filters = append(filters, c.expiresAt.LessThan("", opts.ByExpiresBefore.Unix()))
	}

	var iter record.Iterator[joinTokenObject]
	if len(filters) > 0 {
		iter = record.And(filters)
	} else {
		iter = c.all.Iterate("")
	}

	return iter, nil
}
