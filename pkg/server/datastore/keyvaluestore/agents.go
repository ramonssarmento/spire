package keyvaluestore

import (
	"context"
	"errors"
	"time"

	"github.com/spiffe/spire/pkg/common/protoutil"
	"github.com/spiffe/spire/pkg/server/datastore"
	"github.com/spiffe/spire/pkg/server/datastore/keyvaluestore/internal/record"
	"github.com/spiffe/spire/proto/spire/common"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func (ds *DataStore) CountAttestedNodes(ctx context.Context, req *datastore.CountAttestedNodesRequest) (int32, error) {
	if req.BySelectorMatch != nil && len(req.BySelectorMatch.Selectors) == 0 {
		return -1, status.Error(codes.InvalidArgument, "cannot list by empty selectors set")
	}

	listReq := &listAttestedNodes{
		ListAttestedNodesRequest: datastore.ListAttestedNodesRequest{
			ByAttestationType: req.ByAttestationType,
			ByBanned:          req.ByBanned,
			ByExpiresBefore:   req.ByExpiresBefore,
			BySelectorMatch:   req.BySelectorMatch,
			FetchSelectors:    req.FetchSelectors,
			ByCanReattest:     req.ByCanReattest,
		},
	}

	records, _, err := ds.agents.List(listReq)
	return int32(len(records)), err
}

func (ds *DataStore) CreateAttestedNode(ctx context.Context, in *common.AttestedNode) (*common.AttestedNode, error) {
	if err := ds.agents.Create(ctx, agentObject{AttestedNode: in}); err != nil {
		return nil, dsErr(err, "failed to create agent")
	}

	if err := ds.createAttestedNodeEvent(ctx, &datastore.AttestedNodeEvent{
		SpiffeID: in.SpiffeId,
	}); err != nil {
		return nil, err
	}

	return in, nil
}

func (ds *DataStore) DeleteAttestedNode(ctx context.Context, spiffeID string) (*common.AttestedNode, error) {
	r, err := ds.agents.Get(spiffeID)

	if err != nil {
		return nil, dsErr(err, "failed to delete agent")
	}

	if err := ds.agents.Delete(ctx, spiffeID); err != nil {
		return nil, dsErr(err, "failed to delete agent")
	}

	if err = ds.createAttestedNodeEvent(ctx, &datastore.AttestedNodeEvent{
		SpiffeID: spiffeID,
	}); err != nil {
		return nil, err
	}

	return r.Object.AttestedNode, nil
}

func (ds *DataStore) FetchAttestedNode(ctx context.Context, spiffeID string) (*common.AttestedNode, error) {
	r, err := ds.agents.Get(spiffeID)
	switch {
	case err == nil:
		return r.Object.AttestedNode, nil
	case errors.Is(err, record.ErrNotFound):
		return nil, nil
	default:
		return nil, dsErr(err, "failed to agent bundle")
	}
}

func (ds *DataStore) ListAttestedNodes(ctx context.Context, req *datastore.ListAttestedNodesRequest) (*datastore.ListAttestedNodesResponse, error) {
	records, cursor, err := ds.agents.List(&listAttestedNodes{
		ListAttestedNodesRequest: *req,
	})
	if err != nil {
		return nil, err
	}
	resp := &datastore.ListAttestedNodesResponse{
		Pagination: newPagination(req.Pagination, cursor),
	}
	resp.Nodes = make([]*common.AttestedNode, 0, len(records))
	for _, record := range records {
		resp.Nodes = append(resp.Nodes, record.Object.AttestedNode)
	}
	return resp, nil
}

func (ds *DataStore) UpdateAttestedNode(ctx context.Context, newAgent *common.AttestedNode, mask *common.AttestedNodeMask) (*common.AttestedNode, error) {
	record, err := ds.agents.Get(newAgent.SpiffeId)
	if err != nil {
		return nil, dsErr(err, "failed to update agent")
	}
	existing := record.Object

	if mask == nil {
		mask = protoutil.AllTrueCommonAgentMask
	}

	if mask.CertNotAfter {
		existing.CertNotAfter = newAgent.CertNotAfter
	}
	if mask.CertSerialNumber {
		existing.CertSerialNumber = newAgent.CertSerialNumber
	}
	if mask.NewCertNotAfter {
		existing.NewCertNotAfter = newAgent.NewCertNotAfter
	}
	if mask.NewCertSerialNumber {
		existing.NewCertSerialNumber = newAgent.NewCertSerialNumber
	}
	if mask.CanReattest {
		existing.CanReattest = newAgent.CanReattest
	}
	/*if mask.AttestationDataType {
		existing.AttestationDataType = newAgent.AttestationDataType
	}*/

	if err := ds.agents.Update(ctx, existing, record.Metadata.Revision); err != nil {
		return nil, dsErr(err, "failed to update agent")
	}

	if err = ds.createAttestedNodeEvent(ctx, &datastore.AttestedNodeEvent{
		SpiffeID: newAgent.SpiffeId,
	}); err != nil {
		return nil, err
	}

	return existing.AttestedNode, nil
}

func (ds *DataStore) GetNodeSelectors(ctx context.Context, spiffeID string, dataConsistency datastore.DataConsistency) ([]*common.Selector, error) {
	record, err := ds.agents.Get(spiffeID)
	if err != nil {
		return nil, dsErr(err, "failed to get agent selectors")
	}
	return record.Object.Selectors, nil
}

func (ds *DataStore) ListNodeSelectors(ctx context.Context, req *datastore.ListNodeSelectorsRequest) (*datastore.ListNodeSelectorsResponse, error) {
	records, _, err := ds.agents.List(&listAttestedNodes{
		ByExpiresAfter: req.ValidAt,
	})
	if err != nil {
		return nil, err
	}
	resp := &datastore.ListNodeSelectorsResponse{
		Selectors: map[string][]*common.Selector{},
	}
	for _, record := range records {
		resp.Selectors[record.Object.SpiffeId] = record.Object.Selectors
	}
	return resp, nil
}

func (ds *DataStore) SetNodeSelectors(ctx context.Context, spiffeID string, selectors []*common.Selector) error {
	agent, err := ds.FetchAttestedNode(ctx, spiffeID)
	switch {
	case err != nil:
		return err
	case agent == nil:
		_, err = ds.CreateAttestedNode(ctx, &common.AttestedNode{SpiffeId: spiffeID, Selectors: selectors})
		return err
	default:
		_, err = ds.UpdateAttestedNode(ctx, &common.AttestedNode{SpiffeId: spiffeID, Selectors: selectors}, &common.AttestedNodeMask{})
		return err
	}
}

type agentCodec struct{}

func (agentCodec) Marshal(in *agentObject) (string, []byte, error) {
	out, err := proto.Marshal(in.AttestedNode)
	if err != nil {
		return "", nil, err
	}
	return in.AttestedNode.SpiffeId, out, nil
}

func (agentCodec) Unmarshal(in []byte, out *agentObject) error {
	attestedNode := new(common.AttestedNode)
	if err := proto.Unmarshal(in, attestedNode); err != nil {
		return err
	}
	out.AttestedNode = attestedNode
	return nil
}

type agentObject struct {
	*common.AttestedNode
}

func (r agentObject) Key() string { return r.AttestedNode.SpiffeId }

type listAttestedNodes struct {
	datastore.ListAttestedNodesRequest
	ByExpiresAfter time.Time
}

type agentIndex struct {
	all             record.Set[agentObject]
	attestationType record.UnaryIndex[agentObject, string]
	banned          record.UnaryIndexCmp[agentObject, bool, boolCmp]
	expiresAt       record.UnaryIndex[agentObject, int64]
	selectors       record.MultiIndexCmp[agentObject, *common.Selector, selectorCmp]
	canReattest     record.UnaryIndexCmp[agentObject, bool, boolCmp]
}

func (idx *agentIndex) Count() int {
	return idx.all.Count()
}

func (idx *agentIndex) Get(key string) (*record.Record[agentObject], bool) {
	return idx.all.Get(key)
}

func (idx *agentIndex) Put(r *record.Record[agentObject]) error {
	idx.all.Set(r)
	idx.attestationType.Set(r, r.Object.AttestationDataType)
	idx.banned.Set(r, r.Object.CertSerialNumber == "" && r.Object.NewCertSerialNumber == "")
	idx.expiresAt.Set(r, r.Object.CertNotAfter)
	idx.selectors.Set(r, r.Object.Selectors)
	idx.canReattest.Set(r, r.Object.CanReattest)
	return nil
}

func (idx *agentIndex) Delete(key string) {
	idx.all.Delete(key)
	idx.attestationType.Delete(key)
	idx.banned.Delete(key)
	idx.expiresAt.Delete(key)
	idx.selectors.Delete(key)
	idx.canReattest.Delete(key)
}

func (idx *agentIndex) List(req *listAttestedNodes) (record.Iterator[agentObject], error) {
	cursor, limit, err := getPaginationParams(req.Pagination)
	if err != nil {
		return nil, err
	}

	var filters []record.Iterator[agentObject]
	if req.ByAttestationType != "" {
		filters = append(filters, idx.attestationType.EqualTo(cursor, req.ByAttestationType))
	}
	if req.ByBanned != nil {
		filters = append(filters, idx.banned.EqualTo(cursor, *req.ByBanned))
	}
	if !req.ByExpiresBefore.IsZero() {
		filters = append(filters, idx.expiresAt.LessThan(cursor, req.ByExpiresBefore.Unix()))
	}
	if !req.ByExpiresAfter.IsZero() {
		filters = append(filters, idx.expiresAt.GreaterThan(cursor, req.ByExpiresAfter.Unix()))
	}
	if req.BySelectorMatch != nil {
		filters = append(filters, idx.selectors.Matching(cursor, req.BySelectorMatch.Selectors, matchBehavior(req.BySelectorMatch.Match)))
	}
	if req.ByCanReattest != nil {
		filters = append(filters, idx.canReattest.EqualTo(cursor, *req.ByCanReattest))
	}

	var iter record.Iterator[agentObject]
	if len(filters) > 0 {
		iter = record.And(filters)
	} else {
		iter = idx.all.Iterate(cursor)
	}

	if limit > 0 {
		iter = record.Limit(iter, limit)
	}
	return iter, nil
}
