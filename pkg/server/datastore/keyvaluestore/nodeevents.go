package keyvaluestore

import (
	"context"
	"errors"
	"time"

	"encoding/json"
	"github.com/spiffe/spire/pkg/server/datastore"
	"github.com/spiffe/spire/pkg/server/datastore/keyvaluestore/internal/record"
	"strconv"
)

func (ds *DataStore) ListAttestedNodesEvents(ctx context.Context, req *datastore.ListAttestedNodesEventsRequest) (*datastore.ListAttestedNodesEventsResponse, error) {
	records, _, err := ds.nodeEvents.List(&listAttestedNodesEventsRequest{
		ListAttestedNodesEventsRequest: *req,
	})

	if err != nil {
		return nil, err
	}

	resp := &datastore.ListAttestedNodesEventsResponse{}

	resp.Events = make([]datastore.AttestedNodeEvent, 0, len(records))
	for _, record := range records {
		resp.Events = append(resp.Events, *record.Object.NodeEvent)
	}
	return resp, nil
}

func (ds *DataStore) PruneAttestedNodesEvents(ctx context.Context, olderThan time.Duration) error {
	records, _, err := ds.nodeEvents.List(&listAttestedNodesEventsRequest{
		ByCreatedBefore: time.Now().Add(-olderThan),
	})
	if err != nil {
		return err
	}

	var errCount int
	var firstErr error
	for _, record := range records {
		if err := ds.nodeEvents.Delete(ctx, record.Object.contentKey); err != nil {
			if firstErr == nil {
				firstErr = err
			}
			errCount++
		}
	}

	if firstErr != nil {
		return dsErr(firstErr, "failed pruning %d of %d attested node events: first error:", errCount, len(records))
	}
	return nil
}

func (ds *DataStore) FetchAttestedNodeEvent(ctx context.Context, eventID uint) (*datastore.AttestedNodeEvent, error) {
	r, err := ds.nodeEvents.Get(eventIDtoKey(eventID))
	switch {
	case err == nil:
		return r.Object.NodeEvent, nil
	case errors.Is(err, record.ErrNotFound):
		return nil, nil
	default:
		return nil, dsErr(err, "failed to fetch attested node event")
	}
}

func (ds *DataStore) CreateAttestedNodeEventForTesting(ctx context.Context, event *datastore.AttestedNodeEvent) error {
	return ds.createAttestedNodeEvent(ctx, event)
}

func (ds *DataStore) createAttestedNodeEvent(ctx context.Context, event *datastore.AttestedNodeEvent) error {
	id, err := ds.store.AtomicCounter(ctx, ds.nodeEvents.Kind())
	if err != nil {
		return dsErr(err, "failed to create attested node event")
	}
	event.EventID = id

	if err := ds.nodeEvents.Create(ctx, makeNodeEventObject(event)); err != nil {
		return dsErr(err, "failed to create attested node event")
	}

	return nil
}

func (ds *DataStore) DeleteAttestedNodeEventForTesting(ctx context.Context, eventID uint) error {
	return ds.deleteAttestedNodeEventForTesting(ctx, eventID)
}

func (ds *DataStore) deleteAttestedNodeEventForTesting(ctx context.Context, eventID uint) error {
	if err := ds.nodeEvents.Delete(ctx, eventIDtoKey(eventID)); err != nil {
		return dsErr(err, "failed to delete attested node event")
	}

	return nil
}

type attestedNodeEventWrapper struct {
	EventID  uint   `json:"eventID"`
	SpiffeID string `json:"spiffeID"`
}

type nodeEventCodec struct{}

func (nodeEventCodec) Marshal(in *nodeEventObject) (string, []byte, error) {
	wrappedEvent := &attestedNodeEventWrapper{
		EventID:  in.NodeEvent.EventID,
		SpiffeID: in.NodeEvent.SpiffeID,
	}

	out, err := json.Marshal(wrappedEvent)
	if err != nil {
		return "", nil, err
	}
	return in.contentKey, out, nil
}

func (nodeEventCodec) Unmarshal(in []byte, out *nodeEventObject) error {
	wrappedNode := new(attestedNodeEventWrapper)

	if err := json.Unmarshal(in, wrappedNode); err != nil {
		return err
	}

	out.NodeEvent = &datastore.AttestedNodeEvent{
		EventID:  wrappedNode.EventID,
		SpiffeID: wrappedNode.SpiffeID,
	}

	out.contentKey = eventIDtoKey(out.NodeEvent.EventID)
	return nil
}

type nodeEventObject struct {
	contentKey string
	NodeEvent  *datastore.AttestedNodeEvent
}

func makeNodeEventObject(event *datastore.AttestedNodeEvent) nodeEventObject {
	return nodeEventObject{
		contentKey: eventIDtoKey(event.EventID),
		NodeEvent:  event,
	}
}

func (r nodeEventObject) Key() string { return r.contentKey }

func eventIDtoKey(eventID uint) string {
	return strconv.FormatUint(uint64(eventID), 10)
}

type listAttestedNodesEventsRequest struct {
	datastore.ListAttestedNodesEventsRequest
	ByCreatedBefore time.Time
}

type nodeEventIndex struct {
	all       record.Set[nodeEventObject]
	eventID   record.UnaryIndex[nodeEventObject, uint]
	createdAt record.UnaryIndex[nodeEventObject, int64]
}

func (idx *nodeEventIndex) Count() int {
	return idx.all.Count()
}

func (idx *nodeEventIndex) Get(key string) (*record.Record[nodeEventObject], bool) {
	return idx.all.Get(key)
}

func (idx *nodeEventIndex) Put(r *record.Record[nodeEventObject]) error {
	idx.all.Set(r)
	idx.eventID.Set(r, r.Object.NodeEvent.EventID)
	idx.createdAt.Set(r, r.Metadata.CreatedAt.Unix())
	return nil
}

func (idx *nodeEventIndex) Delete(key string) {
	idx.all.Delete(key)
	idx.createdAt.Delete(key)
	idx.eventID.Delete(key)
}

func (idx *nodeEventIndex) List(req *listAttestedNodesEventsRequest) (record.Iterator[nodeEventObject], error) {
	cursor := ""

	var filters []record.Iterator[nodeEventObject]

	if req.LessThanEventID != 0 {
		filters = append(filters, idx.eventID.LessThan(cursor, req.LessThanEventID))
	}

	if req.GreaterThanEventID != 0 {
		filters = append(filters, idx.eventID.GreaterThan(cursor, req.GreaterThanEventID))
	}

	if !req.ByCreatedBefore.IsZero() {
		filters = append(filters, idx.createdAt.LessThan(cursor, req.ByCreatedBefore.Unix()))
	}

	var iter record.Iterator[nodeEventObject]
	if len(filters) > 0 {
		iter = record.And(filters)
	} else {
		iter = idx.all.Iterate(cursor)
	}

	return iter, nil
}
