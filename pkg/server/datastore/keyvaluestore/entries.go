package keyvaluestore

import (
	"context"
	"errors"
	"time"
	"unicode"

	"github.com/gofrs/uuid/v5"
	"github.com/sirupsen/logrus"
	"github.com/spiffe/spire/pkg/common/telemetry"
	"github.com/spiffe/spire/pkg/server/datastore"
	"github.com/spiffe/spire/pkg/server/datastore/keyvaluestore/internal/record"
	"github.com/spiffe/spire/proto/spire/common"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func (ds *DataStore) CountRegistrationEntries(ctx context.Context, req *datastore.CountRegistrationEntriesRequest) (int32, error) {
	if req.BySelectors != nil && len(req.BySelectors.Selectors) == 0 {
		return 0, status.Error(codes.InvalidArgument, "cannot list by empty selector set")
	}

	listReq := &listRegistrationEntries{
		ListRegistrationEntriesRequest: datastore.ListRegistrationEntriesRequest{
			DataConsistency: req.DataConsistency,
			ByParentID:      req.ByParentID,
			BySelectors:     req.BySelectors,
			BySpiffeID:      req.BySpiffeID,
			ByFederatesWith: req.ByFederatesWith,
			ByHint:          req.ByHint,
			ByDownstream:    req.ByDownstream,
		},
	}

	records, _, err := ds.entries.List(listReq)
	return int32(len(records)), err
}

func (ds *DataStore) CreateRegistrationEntry(ctx context.Context, entry *common.RegistrationEntry) (*common.RegistrationEntry, error) {
	if err := validateRegistrationEntry(entry); err != nil {
		return nil, err
	}

	return ds.createRegistrationEntry(ctx, entry)
}

func (ds *DataStore) createRegistrationEntry(ctx context.Context, entry *common.RegistrationEntry) (*common.RegistrationEntry, error) {
	entryID, err := createOrReturnEntryID(entry)
	if err != nil {
		return nil, err
	}

	entry.EntryId = entryID

	if err := ds.entries.Create(ctx, entryObject{Entry: entry}); err != nil {
		return nil, dsErr(err, "failed to create entry")
	}

	if err = ds.createRegistrationEntryEvent(ctx, &datastore.RegistrationEntryEvent{
		EntryID: entry.EntryId,
	}); err != nil {
		return nil, err
	}

	return entry, nil
}

func (ds *DataStore) CreateOrReturnRegistrationEntry(ctx context.Context, entry *common.RegistrationEntry) (*common.RegistrationEntry, bool, error) {
	if err := validateRegistrationEntry(entry); err != nil {
		return nil, false, err
	}

	records, _, err := ds.entries.List(&listRegistrationEntries{
		ListRegistrationEntriesRequest: datastore.ListRegistrationEntriesRequest{
			BySpiffeID: entry.SpiffeId,
			ByParentID: entry.ParentId,
			BySelectors: &datastore.BySelectors{
				Match:     datastore.Exact,
				Selectors: entry.Selectors,
			},
		},
	})

	if err != nil && len(records) > 0 {
		return records[0].Object.Entry, true, nil
	}

	newEntry, err := ds.createRegistrationEntry(ctx, entry)
	if err != nil {
		return nil, false, err
	}
	return newEntry, false, err
}

func (ds *DataStore) DeleteRegistrationEntry(ctx context.Context, entryID string) (*common.RegistrationEntry, error) {
	r, err := ds.entries.Get(entryID)

	if err != nil {
		return nil, dsErr(err, "failed to delete entry")
	}

	if err := ds.entries.Delete(ctx, entryID); err != nil {
		return nil, dsErr(err, "failed to delete entry")
	}

	if ds.createRegistrationEntryEvent(ctx, &datastore.RegistrationEntryEvent{
		EntryID: entryID,
	}); err != nil {
		return nil, err
	}

	return r.Object.Entry, nil
}

func (ds *DataStore) FetchRegistrationEntry(ctx context.Context, entryID string) (*common.RegistrationEntry, error) {
	r, err := ds.entries.Get(entryID)
	switch {
	case err == nil:
		return r.Object.Entry, nil
	case errors.Is(err, record.ErrNotFound):
		return nil, nil
	default:
		return nil, dsErr(err, "failed to fetch entry")
	}
}

func (ds *DataStore) ListRegistrationEntries(ctx context.Context, req *datastore.ListRegistrationEntriesRequest) (*datastore.ListRegistrationEntriesResponse, error) {
	records, cursor, err := ds.entries.List(&listRegistrationEntries{
		ListRegistrationEntriesRequest: *req,
	})
	if err != nil {
		return nil, err
	}
	resp := &datastore.ListRegistrationEntriesResponse{
		Pagination: newPagination(req.Pagination, cursor),
	}
	resp.Entries = make([]*common.RegistrationEntry, 0, len(records))
	for _, record := range records {
		resp.Entries = append(resp.Entries, record.Object.Entry)
	}
	return resp, nil
}

func (ds *DataStore) PruneRegistrationEntries(ctx context.Context, expiresBefore time.Time) error {
	records, _, err := ds.entries.List(&listRegistrationEntries{
		ByExpiresBefore: expiresBefore,
	})
	if err != nil {
		return err
	}

	var errCount int
	var firstErr error
	for _, record := range records {
		entry := record.Object.Entry
		if err := ds.entries.Delete(ctx, entry.EntryId); err != nil {
			if firstErr == nil {
				firstErr = err
			}
			errCount++
		}

		if err := ds.createRegistrationEntryEvent(ctx, &datastore.RegistrationEntryEvent{
			EntryID: entry.EntryId,
		}); err != nil {
			return err
		}
		ds.log.WithFields(logrus.Fields{
			telemetry.SPIFFEID:       entry.SpiffeId,
			telemetry.ParentID:       entry.ParentId,
			telemetry.RegistrationID: entry.EntryId,
		}).Info("Pruned an expired registration")
	}

	if firstErr != nil {
		return dsErr(firstErr, "failed pruning %d of %d entries: first error:", errCount, len(records))
	}
	return nil
}

func createOrReturnEntryID(entry *common.RegistrationEntry) (string, error) {
	if entry.EntryId != "" {
		return entry.EntryId, nil
	}

	return newRegistrationEntryID()
}

func newRegistrationEntryID() (string, error) {
	u, err := uuid.NewV4()
	if err != nil {
		return "", err
	}
	return u.String(), nil
}

func (ds *DataStore) UpdateRegistrationEntry(ctx context.Context, newEntry *common.RegistrationEntry, mask *common.RegistrationEntryMask) (*common.RegistrationEntry, error) {
	if err := validateRegistrationEntryForUpdate(newEntry, mask); err != nil {
		return nil, dsErr(err, "failed to update entry")
	}

	existing, err := ds.entries.Get(newEntry.EntryId)
	if err != nil {
		return nil, dsErr(err, "failed to update entry")
	}

	updated := existing.Object

	if mask == nil || mask.StoreSvid {
		updated.Entry.StoreSvid = newEntry.StoreSvid
	}

	if mask == nil || mask.Selectors {
		updated.Entry.Selectors = newEntry.Selectors
	}

	if mask == nil || mask.DnsNames {
		updated.Entry.DnsNames = newEntry.DnsNames
	}

	if mask == nil || mask.SpiffeId {
		updated.Entry.SpiffeId = newEntry.SpiffeId
	}

	if mask == nil || mask.ParentId {
		updated.Entry.ParentId = newEntry.ParentId
	}

	if mask == nil || mask.X509SvidTtl {
		updated.Entry.X509SvidTtl = newEntry.X509SvidTtl
	}

	if mask == nil || mask.Admin {
		updated.Entry.Admin = newEntry.Admin
	}

	if mask == nil || mask.Downstream {
		updated.Entry.Downstream = newEntry.Downstream
	}

	if mask == nil || mask.EntryExpiry {
		updated.Entry.EntryExpiry = newEntry.EntryExpiry
	}

	if mask == nil || mask.JwtSvidTtl {
		updated.Entry.JwtSvidTtl = newEntry.JwtSvidTtl
	}

	if mask == nil || mask.Hint {
		updated.Entry.Hint = newEntry.Hint
	}

	if mask == nil || mask.FederatesWith {
		updated.Entry.FederatesWith = newEntry.FederatesWith
	}

	if err := ds.entries.Update(ctx, updated, existing.Metadata.Revision); err != nil {
		return nil, dsErr(err, "failed to update entry")
	}

	if err = ds.createRegistrationEntryEvent(ctx, &datastore.RegistrationEntryEvent{
		EntryID: newEntry.EntryId,
	}); err != nil {
		return nil, err
	}

	return updated.Entry, nil
}

func validateRegistrationEntry(entry *common.RegistrationEntry) error {
	if entry == nil {
		return kvError.New("invalid request: missing registered entry")
	}

	if len(entry.Selectors) == 0 {
		return kvError.New("invalid registration entry: missing selector list")
	}

	// In case of StoreSvid is set, all entries 'must' be the same type,
	// it is done to avoid users to mix selectors from different platforms in
	// entries with storable SVIDs
	if entry.StoreSvid {
		// Selectors must never be empty
		tpe := entry.Selectors[0].Type
		for _, t := range entry.Selectors {
			if tpe != t.Type {
				return kvError.New("invalid registration entry: selector types must be the same when store SVID is enabled")
			}
		}
	}

	if len(entry.EntryId) > 255 {
		return kvError.New("invalid registration entry: entry ID too long")
	}

	for _, e := range entry.EntryId {
		if !unicode.In(e, validEntryIDChars) {
			return kvError.New("invalid registration entry: entry ID contains invalid characters")
		}
	}

	if len(entry.SpiffeId) == 0 {
		return kvError.New("invalid registration entry: missing SPIFFE ID")
	}

	if entry.X509SvidTtl < 0 {
		return kvError.New("invalid registration entry: X509SvidTtl is not set")
	}

	if entry.JwtSvidTtl < 0 {
		return kvError.New("invalid registration entry: JwtSvidTtl is not set")
	}

	return nil
}

func validateRegistrationEntryForUpdate(entry *common.RegistrationEntry, mask *common.RegistrationEntryMask) error {
	if entry == nil {
		return kvError.New("invalid request: missing registered entry")
	}

	if (mask == nil || mask.Selectors) && len(entry.Selectors) == 0 {
		return kvError.New("invalid registration entry: missing selector list")
	}

	if (mask == nil || mask.SpiffeId) &&
		entry.SpiffeId == "" {
		return kvError.New("invalid registration entry: missing SPIFFE ID")
	}

	if (mask == nil || mask.X509SvidTtl) &&
		(entry.X509SvidTtl < 0) {
		return kvError.New("invalid registration entry: X509SvidTtl is not set")
	}

	if (mask == nil || mask.JwtSvidTtl) &&
		(entry.JwtSvidTtl < 0) {
		return kvError.New("invalid registration entry: JwtSvidTtl is not set")
	}

	return nil
}

type entryObject struct {
	Entry *common.RegistrationEntry
}

func (o entryObject) Key() string {
	return o.Entry.EntryId
}

type entryCodec struct{}

func (entryCodec) Marshal(in *entryObject) (string, []byte, error) {
	out, err := proto.Marshal(in.Entry)
	if err != nil {
		return "", nil, err
	}
	return in.Entry.EntryId, out, nil
}

func (entryCodec) Unmarshal(in []byte, out *entryObject) error {
	entry := new(common.RegistrationEntry)
	if err := proto.Unmarshal(in, entry); err != nil {
		return err
	}
	out.Entry = entry
	return nil
}

type listRegistrationEntries struct {
	datastore.ListRegistrationEntriesRequest
	ByExpiresBefore time.Time
}

type entryIndex struct {
	all           record.Set[entryObject]
	parentID      record.UnaryIndex[entryObject, string]
	spiffeID      record.UnaryIndex[entryObject, string]
	selectors     record.MultiIndexCmp[entryObject, *common.Selector, selectorCmp]
	federatesWith record.MultiIndex[entryObject, string]
	expiresAt     record.UnaryIndex[entryObject, int64]
	hint          record.UnaryIndex[entryObject, string]
	downstream    record.UnaryIndexCmp[entryObject, bool, boolCmp]
}

func (c *entryIndex) Count() int {
	return c.all.Count()
}

func (c *entryIndex) Get(key string) (*record.Record[entryObject], bool) {
	return c.all.Get(key)
}

func (c *entryIndex) Put(r *record.Record[entryObject]) error {
	r.Object.Entry.RevisionNumber = r.Metadata.Revision

	c.all.Set(r)
	c.parentID.Set(r, r.Object.Entry.ParentId)
	c.spiffeID.Set(r, r.Object.Entry.SpiffeId)
	c.selectors.Set(r, r.Object.Entry.Selectors)
	c.federatesWith.Set(r, r.Object.Entry.FederatesWith)
	c.expiresAt.Set(r, r.Object.Entry.EntryExpiry)
	c.hint.Set(r, r.Object.Entry.Hint)
	c.downstream.Set(r, r.Object.Entry.Downstream)
	return nil
}

func (c *entryIndex) Delete(key string) {
	c.all.Delete(key)
	c.parentID.Delete(key)
	c.spiffeID.Delete(key)
	c.selectors.Delete(key)
	c.federatesWith.Delete(key)
	c.expiresAt.Delete(key)
	c.hint.Delete(key)
	c.downstream.Delete(key)
}

func (c *entryIndex) List(req *listRegistrationEntries) (record.Iterator[entryObject], error) {
	cursor, limit, err := getPaginationParams(req.Pagination)
	if err != nil {
		return nil, err
	}

	var filters []record.Iterator[entryObject]
	if req.ByParentID != "" {
		filters = append(filters, c.parentID.EqualTo(cursor, req.ByParentID))
	}
	if req.BySelectors != nil {
		filters = append(filters, c.selectors.Matching(cursor, req.BySelectors.Selectors, matchBehavior(req.BySelectors.Match)))
	}
	if req.BySpiffeID != "" {
		filters = append(filters, c.spiffeID.EqualTo(cursor, req.BySpiffeID))
	}
	if req.ByFederatesWith != nil {
		filters = append(filters, c.federatesWith.Matching(cursor, req.ByFederatesWith.TrustDomains, matchBehavior(req.ByFederatesWith.Match)))
	}
	if req.ByHint != "" {
		filters = append(filters, c.hint.EqualTo(cursor, req.ByHint))
	}
	if req.ByDownstream != nil {
		filters = append(filters, c.downstream.EqualTo(cursor, *req.ByDownstream))
	}
	
	if !req.ByExpiresBefore.IsZero() {
		filters = append(filters, c.expiresAt.LessThan(cursor, req.ByExpiresBefore.Unix()))
	}
	
	var iter record.Iterator[entryObject]
	if len(filters) > 0 {
		iter = record.And(filters)
	} else {
		iter = c.all.Iterate(cursor)
	}

	if limit > 0 {
		iter = record.Limit(iter, limit)
	}
	return iter, nil
}
