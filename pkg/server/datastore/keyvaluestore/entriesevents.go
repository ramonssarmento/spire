package keyvaluestore

import (
	"context"
	"errors"
	"time"

	"encoding/json"
	"github.com/spiffe/spire/pkg/server/datastore"
	"github.com/spiffe/spire/pkg/server/datastore/keyvaluestore/internal/keyvalue"
	"github.com/spiffe/spire/pkg/server/datastore/keyvaluestore/internal/record"
	"strconv"
)

func (ds *DataStore) ListRegistrationEntryEvents(ctx context.Context, req *datastore.ListRegistrationEntryEventsRequest) (*datastore.ListRegistrationEntryEventsResponse, error) {
	records, _, err := ds.entriesEvents.List(ctx, &listRegistrationEntryEventsRequest{
		ListRegistrationEntryEventsRequest: *req,
	})

	if err != nil {
		return nil, err
	}

	resp := &datastore.ListRegistrationEntryEventsResponse{}

	resp.Events = make([]datastore.RegistrationEntryEvent, 0, len(records))
	for _, record := range records {
		resp.Events = append(resp.Events, *record.Object.EntryEvent)
	}
	return resp, nil
}

func (ds *DataStore) PruneRegistrationEntryEvents(ctx context.Context, olderThan time.Duration) error {
	records, _, err := ds.entriesEvents.List(ctx, &listRegistrationEntryEventsRequest{
		ByCreatedBefore: time.Now().Add(-olderThan),
	})
	if err != nil {
		return err
	}

	var errCount int
	var firstErr error
	for _, record := range records {
		if err := ds.entriesEvents.Delete(ctx, record.Object.contentKey); err != nil {
			if firstErr == nil {
				firstErr = err
			}
			errCount++
		}
	}

	if firstErr != nil {
		return dsErr(firstErr, "failed pruning %d of %d entries events: first error:", errCount, len(records))
	}
	return nil
}

func (ds *DataStore) FetchRegistrationEntryEvent(ctx context.Context, eventID uint) (*datastore.RegistrationEntryEvent, error) {
	r, err := ds.entriesEvents.Get(ctx, entryEventContentKey(eventID))
	switch {
	case err == nil:
		return r.Object.EntryEvent, nil
	case errors.Is(err, record.ErrNotFound):
		return nil, nil
	default:
		return nil, dsErr(err, "failed to fetch entry event")
	}
}

func (ds *DataStore) CreateRegistrationEntryEventForTesting(ctx context.Context, event *datastore.RegistrationEntryEvent) error {
	return ds.createRegistrationEntryEvent(ctx, event)
}

func (ds *DataStore) createRegistrationEntryEvent(ctx context.Context, event *datastore.RegistrationEntryEvent) error {
	id, err := ds.store.AtomicCounter(ctx, ds.entriesEvents.Kind())
	if err != nil {
		return dsErr(err, "failed to create entry event")
	}
	event.EventID = id

	if err := ds.entriesEvents.Create(ctx, makeEntryEventObject(event)); err != nil {
		return dsErr(err, "failed to create entry event")
	}

	return nil
}

func (ds *DataStore) DeleteRegistrationEntryEventForTesting(ctx context.Context, eventID uint) error {
	return ds.deleteRegistrationEntryEvent(ctx, eventID)
}
func (ds *DataStore) deleteRegistrationEntryEvent(ctx context.Context, eventID uint) error {
	if err := ds.entriesEvents.Delete(ctx, entryEventContentKey(eventID)); err != nil {
		return dsErr(err, "failed to delete entry event")
	}

	return nil
}

type RegistrationEntryEventWrapper struct {
	EventID uint   `json:"eventID"`
	EntryID string `json:"entryID"`
}

type entryEventCodec struct{}

func (entryEventCodec) Marshal(in *entryEventObject) (string, []byte, error) {
	// Wrap the EntryEvent in a wrapper
	wrappedEvent := &RegistrationEntryEventWrapper{
		EventID: in.EntryEvent.EventID,
		EntryID: in.EntryEvent.EntryID,
	}

	out, err := json.Marshal(wrappedEvent)
	if err != nil {
		return "", nil, err
	}
	return in.contentKey, out, nil
}

func (entryEventCodec) Unmarshal(in []byte, out *entryEventObject) error {
	wrappedEntry := new(RegistrationEntryEventWrapper)

	if err := json.Unmarshal(in, wrappedEntry); err != nil {
		return err
	}

	// Unwrap the fields into the original EntryEvent
	out.EntryEvent = &datastore.RegistrationEntryEvent{
		EventID: wrappedEntry.EventID,
		EntryID: wrappedEntry.EntryID,
	}

	out.contentKey = entryEventContentKey(out.EntryEvent.EventID)
	return nil
}

type entryEventObject struct {
	contentKey string
	EntryEvent *datastore.RegistrationEntryEvent
}

func makeEntryEventObject(entry *datastore.RegistrationEntryEvent) entryEventObject {
	return entryEventObject{
		contentKey: entryEventContentKey(entry.EventID),
		EntryEvent: entry,
	}
}

func (r entryEventObject) Key() string { return r.contentKey }

type listRegistrationEntryEventsRequest struct {
	datastore.ListRegistrationEntryEventsRequest
	ByCreatedBefore time.Time
}

func entryEventContentKey(eventID uint) string {
	return strconv.FormatUint(uint64(eventID), 10)
}

type entryEventIndex struct {
	eventID   record.UnaryIndex[uint]
	createdAt record.UnaryIndex[int64]
}

func (idx *entryEventIndex) SetUp() {
	idx.eventID.SetQuerry("Object.EntryEvent.EventID")
	idx.createdAt.SetQuerry("Metadata.CreatedAt")
}

func (idx *entryEventIndex) List(req *listRegistrationEntryEventsRequest) (*keyvalue.ListObject, error) {
	list := new(keyvalue.ListObject)

	list.Cursor = ""
	list.Limit = -1

	if req.LessThanEventID != 0 {
		list.Filters = append(list.Filters, idx.eventID.LessThan(req.LessThanEventID))
	}

	if req.GreaterThanEventID != 0 {
		list.Filters = append(list.Filters, idx.eventID.GreaterThan(req.GreaterThanEventID))
	}

	if !req.ByCreatedBefore.IsZero() {
		list.Filters = append(list.Filters, idx.createdAt.LessThan(req.ByCreatedBefore.Unix()))
	}

	return list, nil
}
