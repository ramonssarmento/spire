package keyvaluestore

import (
	"context"
	"strconv"

	"encoding/json"
	"github.com/sirupsen/logrus"
	"github.com/spiffe/spire/pkg/common/telemetry"
	"github.com/spiffe/spire/pkg/server/datastore"
	"github.com/spiffe/spire/pkg/server/datastore/keyvaluestore/internal/record"
	"github.com/spiffe/spire/proto/private/server/journal"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func (ds *DataStore) SetCAJournal(ctx context.Context, caJournal *datastore.CAJournal) (*datastore.CAJournal, error) {
	if err := validateCAJournal(caJournal); err != nil {
		return nil, err
	}

	if caJournal.ID == 0 {
		return ds.createCAJournal(ctx, caJournal)
	}
	return ds.updateCAJournal(ctx, caJournal)
}

func (ds *DataStore) FetchCAJournal(ctx context.Context, activeX509AuthorityID string) (*datastore.CAJournal, error) {
	var records record.Iterator[caJournalObject]

	ds.caJournal.ReadIndex(func(index *caJournalIndex) {
		records = index.x509AuthorityID.EqualTo("", activeX509AuthorityID)
	})
	obj := records.Record()
	if obj != nil && obj.Object.CAJournal.ActiveX509AuthorityID == activeX509AuthorityID {
		return records.Record().Object.CAJournal, nil
	}

	return nil, status.Error(codes.InvalidArgument, "CA journal not found")
}

func (ds *DataStore) PruneCAJournals(ctx context.Context, allAuthoritiesExpireBefore int64) error {
	var errCount int
	var firstErr error

	// TO-DO
	// In future we could store expiration date on Index
	records, _, err := ds.caJournal.List(0)
	if err != nil {
		return dsErr(err, "failed to delete CA journal")
	}

checkAuthorities:
	for _, record := range records {
		model := record.Object.CAJournal

		entries := new(journal.Entries)
		if err = proto.Unmarshal(model.Data, entries); err != nil {
			return status.Errorf(codes.Internal, "unable to unmarshal entries from CA journal record: %v", err)
		}

		for _, x509CA := range entries.X509CAs {
			if x509CA.NotAfter > allAuthoritiesExpireBefore {
				continue checkAuthorities
			}
		}
		for _, jwtKey := range entries.JwtKeys {
			if jwtKey.NotAfter > allAuthoritiesExpireBefore {
				continue checkAuthorities
			}
		}
		if err := ds.caJournal.Delete(ctx, idToKey(model.ID)); err != nil {
			return status.Errorf(codes.Internal, "failed to delete CA journal: %v", err)
		}

		ds.log.WithFields(logrus.Fields{
			telemetry.CAJournalID: model.ID,
		}).Info("Pruned stale CA journal record")
	}

	if firstErr != nil {
		return dsErr(firstErr, "failed pruning %d of %d attested node events: first error:", errCount, len(records))
	}

	return nil
}

func (ds *DataStore) ListCAJournalsForTesting(ctx context.Context) ([]*datastore.CAJournal, error) {
	records, _, err := ds.caJournal.List(0)
	if err != nil {
		return nil, err
	}

	resp := make([]*datastore.CAJournal, 0, len(records))
	for _, record := range records {
		resp = append(resp, record.Object.CAJournal)
	}
	return resp, nil
}

func validateCAJournal(caJournal *datastore.CAJournal) error {
	if caJournal == nil {
		return status.Error(codes.InvalidArgument, "CA journal is required")
	}
	return nil
}

func (ds *DataStore) createCAJournal(ctx context.Context, caJournal *datastore.CAJournal) (*datastore.CAJournal, error) {
	id, err := ds.store.AtomicCounter(ctx, ds.caJournal.Kind())
	if err != nil {
		return nil, dsErr(err, "failed to create CA Journal")
	}
	caJournal.ID = id

	if err := ds.caJournal.Create(ctx, makeCAJournalObject(caJournal)); err != nil {
		return nil, dsErr(err, "failed to create CA Journal")
	}

	return caJournal, nil
}

func (ds *DataStore) updateCAJournal(ctx context.Context, caJournal *datastore.CAJournal) (*datastore.CAJournal, error) {
	existing, err := ds.caJournal.Get(idToKey(caJournal.ID))
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "CA journal not found")
	}

	existing.Object.CAJournal.ActiveX509AuthorityID = caJournal.ActiveX509AuthorityID
	existing.Object.CAJournal.Data = caJournal.Data

	if err = ds.caJournal.Update(ctx, existing.Object, existing.Metadata.Revision); err != nil {
		return nil, dsErr(err, "failed to update CA Journal")
	}

	return existing.Object.CAJournal, nil
}

type caJournalObject struct {
	CAJournal  *datastore.CAJournal
	contentKey string
}

func makeCAJournalObject(caJournal *datastore.CAJournal) caJournalObject {
	return caJournalObject{
		contentKey: idToKey(caJournal.ID), // Assuming EventID is not present, use ID instead.
		CAJournal:  caJournal,
	}
}

func (r caJournalObject) Key() string { return r.contentKey }

func idToKey(eventID uint) string {
	return strconv.FormatUint(uint64(eventID), 10)
}

type caJournalWrapper struct {
	ID                    uint   `json:"id"`
	Data                  []byte `json:"data"`
	ActiveX509AuthorityID string `json:"active_x509_authority_id"`
}

type caJournalCodec struct{}

func (caJournalCodec) Marshal(in *caJournalObject) (string, []byte, error) {
	wrappedJournal := &caJournalWrapper{
		ID:                    in.CAJournal.ID,
		Data:                  in.CAJournal.Data,
		ActiveX509AuthorityID: in.CAJournal.ActiveX509AuthorityID,
	}

	out, err := json.Marshal(wrappedJournal)
	if err != nil {
		return "", nil, err
	}
	return in.contentKey, out, nil
}

func (caJournalCodec) Unmarshal(in []byte, out *caJournalObject) error {
	wrappedJournal := new(caJournalWrapper)

	if err := json.Unmarshal(in, wrappedJournal); err != nil {
		return err
	}

	out.CAJournal = &datastore.CAJournal{
		ID:                    wrappedJournal.ID,
		ActiveX509AuthorityID: wrappedJournal.ActiveX509AuthorityID,
		Data:                  wrappedJournal.Data,
	}

	out.contentKey = idToKey(out.CAJournal.ID)
	return nil
}

type caJournalIndex struct {
	all             record.Set[caJournalObject]
	x509AuthorityID record.UnaryIndex[caJournalObject, string]
}

func (c *caJournalIndex) Count() int {
	return c.all.Count()
}

func (c *caJournalIndex) Get(key string) (*record.Record[caJournalObject], bool) {
	return c.all.Get(key)
}

func (c *caJournalIndex) Put(r *record.Record[caJournalObject]) error {
	c.all.Set(r)
	c.x509AuthorityID.Set(r, r.Object.CAJournal.ActiveX509AuthorityID)
	return nil
}

func (c *caJournalIndex) Delete(key string) {
	c.all.Delete(key)
	c.x509AuthorityID.Delete(key)
}

func (c *caJournalIndex) List(allCAsExpireBefore int64) (record.Iterator[caJournalObject], error) {
	return c.all.Iterate(""), nil
}
