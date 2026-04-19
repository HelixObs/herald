package api

import (
	"context"
	"fmt"
	"sort"
	"time"

	graphql "github.com/graph-gophers/graphql-go"

	"github.com/HelixObs/gateway/internal/db"
)

// ── Input types ───────────────────────────────────────────────────────────────
// Optional fields are pointers; graph-gophers sets them to nil when absent.

type entityFilterInput struct {
	InstrumentId *string
	From         *string
	To           *string
	Limit        *int32
	Offset       *int32
}

type operationFilterInput struct {
	Operation *string
	From      *string
	To        *string
	Limit     *int32
}

type eventFilterInput struct {
	EventName *string
	From      *string
	To        *string
	Limit     *int32
}

// ── Root resolver (implements Query) ─────────────────────────────────────────

type rootResolver struct {
	db *db.Store
}

func (r *rootResolver) Entity(ctx context.Context, args struct {
	ID graphql.ID
}) (*entityResolver, error) {
	e, err := r.db.GetEntity(ctx, string(args.ID))
	if err != nil {
		return nil, err
	}
	if e == nil {
		return nil, nil
	}
	return &entityResolver{e: *e, db: r.db}, nil
}

func (r *rootResolver) Entities(ctx context.Context, args struct {
	Filter *entityFilterInput
}) ([]*entityResolver, error) {
	entities, err := r.db.ListEntities(ctx, toEntityFilter(args.Filter))
	if err != nil {
		return nil, err
	}
	out := make([]*entityResolver, len(entities))
	for i, e := range entities {
		out[i] = &entityResolver{e: e, db: r.db}
	}
	return out, nil
}

func (r *rootResolver) EntityOperations(ctx context.Context, args struct {
	EntityId graphql.ID
	Filter   *operationFilterInput
}) ([]*entityOperationResolver, error) {
	ops, err := r.db.ListEntityOperations(ctx, string(args.EntityId), toOperationFilter(args.Filter))
	if err != nil {
		return nil, err
	}
	return wrapOperations(ops), nil
}

func (r *rootResolver) EntityEvents(ctx context.Context, args struct {
	EntityId graphql.ID
	Filter   *eventFilterInput
}) ([]*entityEventResolver, error) {
	evs, err := r.db.ListEntityEvents(ctx, string(args.EntityId), toEventFilter(args.Filter))
	if err != nil {
		return nil, err
	}
	return wrapEvents(evs), nil
}

// ── Entity resolver ───────────────────────────────────────────────────────────

type entityResolver struct {
	e  db.Entity
	db *db.Store
}

func (r *entityResolver) ID() graphql.ID      { return graphql.ID(r.e.ID) }
func (r *entityResolver) InstrumentId() string { return r.e.InstrumentID }
func (r *entityResolver) TraceId() string      { return r.e.TraceID }
func (r *entityResolver) TimestampNs() string  { return fmt.Sprintf("%d", r.e.TimestampNs) }
func (r *entityResolver) ParentIds() []string  { return r.e.ParentIDs }
func (r *entityResolver) Metadata() []*keyValueResolver { return toKV(r.e.Metadata) }
func (r *entityResolver) CreatedAt() string    { return r.e.CreatedAt.UTC().Format(time.RFC3339) }

func (r *entityResolver) Operations(ctx context.Context, args struct {
	Filter *operationFilterInput
}) ([]*entityOperationResolver, error) {
	ops, err := r.db.ListEntityOperations(ctx, r.e.ID, toOperationFilter(args.Filter))
	if err != nil {
		return nil, err
	}
	return wrapOperations(ops), nil
}

func (r *entityResolver) Events(ctx context.Context, args struct {
	Filter *eventFilterInput
}) ([]*entityEventResolver, error) {
	evs, err := r.db.ListEntityEvents(ctx, r.e.ID, toEventFilter(args.Filter))
	if err != nil {
		return nil, err
	}
	return wrapEvents(evs), nil
}

// ── EntityOperation resolver ──────────────────────────────────────────────────

type entityOperationResolver struct{ op db.EntityOperation }

func (r *entityOperationResolver) EntityId() graphql.ID      { return graphql.ID(r.op.EntityID) }
func (r *entityOperationResolver) InstrumentId() string       { return r.op.InstrumentID }
func (r *entityOperationResolver) Operation() string          { return r.op.Operation }
func (r *entityOperationResolver) TraceId() string            { return r.op.TraceID }
func (r *entityOperationResolver) TimestampNs() string        { return fmt.Sprintf("%d", r.op.TimestampNs) }
func (r *entityOperationResolver) Metadata() []*keyValueResolver { return toKV(r.op.Metadata) }
func (r *entityOperationResolver) CreatedAt() string          { return r.op.CreatedAt.UTC().Format(time.RFC3339) }

// ── EntityEvent resolver ──────────────────────────────────────────────────────

type entityEventResolver struct{ ev db.EntityEvent }

func (r *entityEventResolver) EntityId() graphql.ID      { return graphql.ID(r.ev.EntityID) }
func (r *entityEventResolver) InstrumentId() string       { return r.ev.InstrumentID }
func (r *entityEventResolver) EventName() string          { return r.ev.EventName }
func (r *entityEventResolver) TimestampNs() string        { return fmt.Sprintf("%d", r.ev.TimestampNs) }
func (r *entityEventResolver) Metadata() []*keyValueResolver { return toKV(r.ev.Metadata) }
func (r *entityEventResolver) CreatedAt() string          { return r.ev.CreatedAt.UTC().Format(time.RFC3339) }

// ── KeyValue resolver ─────────────────────────────────────────────────────────

type keyValueResolver struct{ key, value string }

func (r *keyValueResolver) Key() string   { return r.key }
func (r *keyValueResolver) Value() string { return r.value }

// toKV converts a metadata map to a sorted slice of KeyValue resolvers.
// Sorted so queries return deterministic output.
func toKV(m map[string]string) []*keyValueResolver {
	out := make([]*keyValueResolver, 0, len(m))
	for k, v := range m {
		out = append(out, &keyValueResolver{key: k, value: v})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].key < out[j].key })
	return out
}

// ── Filter converters ─────────────────────────────────────────────────────────

func parseTime(s *string) time.Time {
	if s == nil || *s == "" {
		return time.Time{}
	}
	t, _ := time.Parse(time.RFC3339, *s)
	return t
}

func derefStr(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

func derefInt(i *int32) int {
	if i == nil {
		return 0
	}
	return int(*i)
}

func toEntityFilter(f *entityFilterInput) db.EntityFilter {
	if f == nil {
		return db.EntityFilter{}
	}
	return db.EntityFilter{
		InstrumentID: derefStr(f.InstrumentId),
		From:         parseTime(f.From),
		To:           parseTime(f.To),
		Limit:        derefInt(f.Limit),
		Offset:       derefInt(f.Offset),
	}
}

func toOperationFilter(f *operationFilterInput) db.OperationFilter {
	if f == nil {
		return db.OperationFilter{}
	}
	return db.OperationFilter{
		Operation: derefStr(f.Operation),
		From:      parseTime(f.From),
		To:        parseTime(f.To),
		Limit:     derefInt(f.Limit),
	}
}

func toEventFilter(f *eventFilterInput) db.EventFilter {
	if f == nil {
		return db.EventFilter{}
	}
	return db.EventFilter{
		EventName: derefStr(f.EventName),
		From:      parseTime(f.From),
		To:        parseTime(f.To),
		Limit:     derefInt(f.Limit),
	}
}

// ── Wrap helpers ──────────────────────────────────────────────────────────────

func wrapOperations(ops []db.EntityOperation) []*entityOperationResolver {
	out := make([]*entityOperationResolver, len(ops))
	for i, op := range ops {
		out[i] = &entityOperationResolver{op: op}
	}
	return out
}

func wrapEvents(evs []db.EntityEvent) []*entityEventResolver {
	out := make([]*entityEventResolver, len(evs))
	for i, ev := range evs {
		out[i] = &entityEventResolver{ev: ev}
	}
	return out
}
