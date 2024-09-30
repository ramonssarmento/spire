package keyvaluestore

import (
	"context"
	"errors"
	"time"

	"github.com/spiffe/spire/pkg/common/bundleutil"
	"github.com/spiffe/spire/pkg/common/protoutil"
	"github.com/spiffe/spire/pkg/server/datastore"
	"github.com/spiffe/spire/pkg/server/datastore/keyvaluestore/internal/record"
	"github.com/spiffe/spire/proto/spire/common"
	"google.golang.org/protobuf/proto"
)

func (ds *DataStore) AppendBundle(ctx context.Context, appends *common.Bundle) (*common.Bundle, error) {
	// TODO: validation
	existing, err := ds.bundles.Get(appends.TrustDomainId)
	switch {
	case err == nil:
		if merged, changed := bundleutil.MergeBundles(existing.Object.Bundle, appends); changed {
			if err := ds.bundles.Update(ctx, bundleObject{Bundle: merged}, existing.Metadata.Revision); err != nil {
				return nil, dsErr(err, "failed to update existing bundle on append")
			}
			return merged, nil
		} else {
			// Bundle didn't change. Return the original.
			return existing.Object.Bundle, nil
		}
	case errors.Is(err, record.ErrNotFound):
		if err := ds.bundles.Create(ctx, bundleObject{Bundle: appends}); err != nil {
			return nil, dsErr(err, "failed to create new bundle on append")
		}
		return appends, nil
	default:
		return nil, dsErr(err, "failed to fetch existing bundle on append")
	}
}

func (ds *DataStore) CountBundles(context.Context) (int32, error) {
	return int32(ds.bundles.Count()), nil
}

func (ds *DataStore) CreateBundle(ctx context.Context, in *common.Bundle) (*common.Bundle, error) {
	if err := ds.bundles.Create(ctx, bundleObject{Bundle: in}); err != nil {
		return nil, dsErr(err, "failed to create bundle")
	}
	return in, nil
}

func (ds *DataStore) DeleteBundle(ctx context.Context, trustDomainID string, mode datastore.DeleteMode) error {
	if err := ds.bundles.Delete(ctx, trustDomainID); err != nil {
		return dsErr(err, "failed to delete bundle")
	}
	return nil
}

func (ds *DataStore) FetchBundle(ctx context.Context, trustDomainID string) (*common.Bundle, error) {
	out, err := ds.bundles.Get(trustDomainID)
	switch {
	case err == nil:
		return out.Object.Bundle, nil
	case errors.Is(err, record.ErrNotFound):
		return nil, nil
	default:
		return nil, dsErr(err, "failed to fetch bundle")
	}
}

func (ds *DataStore) ListBundles(ctx context.Context, req *datastore.ListBundlesRequest) (*datastore.ListBundlesResponse, error) {
	records, cursor, err := ds.bundles.List(req)
	if err != nil {
		return nil, err
	}
	resp := &datastore.ListBundlesResponse{
		Pagination: newPagination(req.Pagination, cursor),
	}
	resp.Bundles = make([]*common.Bundle, 0, len(records))
	for _, record := range records {
		resp.Bundles = append(resp.Bundles, record.Object.Bundle)
	}
	return resp, nil
}

func (ds *DataStore) PruneBundle(ctx context.Context, trustDomainID string, expiresBefore time.Time) (changed bool, err error) {
	// TODO: validation
	r, err := ds.bundles.Get(trustDomainID)
	switch {
	case err == nil:
		pruned, changed, err := bundleutil.PruneBundle(r.Object.Bundle, expiresBefore, ds.log)
		switch {
		case err != nil:
			return false, dsErr(err, "failed to prune bundle")
		case changed:
			// TODO: retry on conflict
			if err := ds.bundles.Update(ctx, bundleObject{Bundle: pruned}, r.Metadata.Revision); err != nil {
				return false, dsErr(err, "failed to update existing bundle on prune")
			}
			return true, nil
		default:
			return false, nil
		}
	case errors.Is(err, record.ErrNotFound):
		return false, nil
	default:
		return false, dsErr(err, "failed to fetch existing bundle on prune")
	}
}

func (ds *DataStore) SetBundle(ctx context.Context, in *common.Bundle) (*common.Bundle, error) {
	bundle, err := ds.bundles.Get(in.TrustDomainId)
	switch {
	case err == nil:
		if err := ds.bundles.Update(ctx, bundleObject{Bundle: in}, bundle.Metadata.Revision); err != nil {
			return nil, dsErr(err, "failed to update bundle on set")
		}
		return in, nil
	case errors.Is(err, record.ErrNotFound):
		if err := ds.bundles.Create(ctx, bundleObject{Bundle: in}); err != nil {
			return nil, dsErr(err, "failed to create bundle on set")
		}
		return in, nil
	default:
		return nil, dsErr(err, "failed to fetch bundle for set")
	}
}

func (ds *DataStore) UpdateBundle(ctx context.Context, newBundle *common.Bundle, mask *common.BundleMask) (*common.Bundle, error) {
	existing, err := ds.bundles.Get(newBundle.TrustDomainId)
	if err != nil {
		return nil, dsErr(err, "failed to update bundle")
	}

	updated := existing.Object

	if mask == nil {
		mask = protoutil.AllTrueCommonBundleMask
	}
	if mask.RefreshHint {
		updated.Bundle.RefreshHint = newBundle.RefreshHint
	}
	if mask.RootCas {
		updated.Bundle.RootCas = newBundle.RootCas
	}
	if mask.JwtSigningKeys {
		updated.Bundle.JwtSigningKeys = newBundle.JwtSigningKeys
	}

	if err := ds.bundles.Update(ctx, updated, existing.Metadata.Revision); err != nil {
		return nil, dsErr(err, "failed to update bundle")
	}
	return updated.Bundle, nil
}

type bundleObject struct {
	Bundle *common.Bundle
}

func (o bundleObject) Key() string {
	if o.Bundle == nil {
		return ""
	}
	return o.Bundle.TrustDomainId
}

type bundleCodec struct{}

func (bundleCodec) Marshal(o *bundleObject) (string, []byte, error) {
	data, err := proto.Marshal(o.Bundle)
	if err != nil {
		return "", nil, err
	}
	return o.Key(), data, nil
}

func (bundleCodec) Unmarshal(in []byte, out *bundleObject) error {
	bundle := new(common.Bundle)
	if err := proto.Unmarshal(in, bundle); err != nil {
		return err
	}
	out.Bundle = bundle
	return nil
}

type bundleIndex struct {
	all record.Set[bundleObject]
}

func (c *bundleIndex) Count() int {
	return c.all.Count()
}

func (c *bundleIndex) Get(key string) (*record.Record[bundleObject], bool) {
	return c.all.Get(key)
}

func (c *bundleIndex) Put(r *record.Record[bundleObject]) error {
	c.all.Set(r)
	return nil
}

func (c *bundleIndex) Delete(key string) {
	c.all.Delete(key)
}

func (c *bundleIndex) List(req *datastore.ListBundlesRequest) (record.Iterator[bundleObject], error) {
	cursor, limit, err := getPaginationParams(req.Pagination)
	if err != nil {
		return nil, err
	}

	iter := c.all.Iterate(cursor)
	if limit > 0 {
		iter = record.Limit(iter, limit)
	}
	return iter, nil
}
