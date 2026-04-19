// Package api exposes a GraphQL query interface over the HelixObs data model.
// The schema is intentionally read-only — all writes go through the OTLP gRPC path.
//
// Endpoints served on the API_ADDR HTTP server:
//
//	POST /graphql   — GraphQL query endpoint (JSON body)
//	GET  /graphql   — same, query via URL param
//	GET  /          — GraphiQL interactive playground
package api

import (
	"net/http"

	graphql "github.com/graph-gophers/graphql-go"
	"github.com/graph-gophers/graphql-go/relay"

	"github.com/HelixObs/gateway/internal/db"
)

const schemaStr = `
	schema { query: Query }

	type Query {
		"""Look up a single entity by its ID."""
		entity(id: ID!): Entity

		"""List entities, newest-first. Supports filtering and pagination."""
		entities(filter: EntityFilter): [Entity!]!

		"""List all operations recorded against an entity, oldest-first."""
		entityOperations(entityId: ID!, filter: OperationFilter): [EntityOperation!]!

		"""List all helix.* span events recorded against an entity, oldest-first."""
		entityEvents(entityId: ID!, filter: EventFilter): [EntityEvent!]!
	}

	type Entity {
		id:           ID!
		instrumentId: String!
		traceId:      String!
		timestampNs:  String!
		parentIds:    [String!]!
		metadata:     [KeyValue!]!
		createdAt:    String!
		"""Nested convenience: fetch this entity's operations in the same query."""
		operations(filter: OperationFilter): [EntityOperation!]!
		"""Nested convenience: fetch this entity's events in the same query."""
		events(filter: EventFilter): [EntityEvent!]!
	}

	type EntityOperation {
		entityId:     ID!
		instrumentId: String!
		operation:    String!
		traceId:      String!
		timestampNs:  String!
		metadata:     [KeyValue!]!
		createdAt:    String!
	}

	type EntityEvent {
		entityId:     ID!
		instrumentId: String!
		eventName:    String!
		timestampNs:  String!
		metadata:     [KeyValue!]!
		createdAt:    String!
	}

	"""A single metadata key-value pair (stored as JSONB in the DB)."""
	type KeyValue {
		key:   String!
		value: String!
	}

	input EntityFilter {
		"""Filter by instrument (e.g. "CHIME")."""
		instrumentId: String
		"""RFC 3339 lower bound on created_at."""
		from: String
		"""RFC 3339 upper bound on created_at."""
		to: String
		"""Max rows to return (default 100, max 1000)."""
		limit: Int
		"""Rows to skip for pagination."""
		offset: Int
	}

	input OperationFilter {
		"""Filter by operation name (e.g. "hdf5-conversion")."""
		operation: String
		from:  String
		to:    String
		limit: Int
	}

	input EventFilter {
		"""Filter by event name (e.g. "helix.error")."""
		eventName: String
		from:  String
		to:    String
		limit: Int
	}
`

// NewHandler returns an http.Handler that serves the GraphQL API and GraphiQL playground.
func NewHandler(store *db.Store) http.Handler {
	schema := graphql.MustParseSchema(schemaStr, &rootResolver{db: store},
		graphql.UseFieldResolvers(),
	)
	mux := http.NewServeMux()
	mux.Handle("/graphql", cors(&relay.Handler{Schema: schema}))
	mux.HandleFunc("/", servePlayground)
	return mux
}

// cors adds permissive CORS headers so a browser-hosted UI on any origin can query.
func cors(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

const playgroundHTML = `<!DOCTYPE html>
<html>
<head>
  <title>HelixObs GraphQL</title>
  <link rel="stylesheet" href="https://unpkg.com/graphiql/graphiql.min.css"/>
</head>
<body style="margin:0">
  <div id="graphiql" style="height:100vh"></div>
  <script crossorigin src="https://unpkg.com/react/umd/react.production.min.js"></script>
  <script crossorigin src="https://unpkg.com/react-dom/umd/react-dom.production.min.js"></script>
  <script crossorigin src="https://unpkg.com/graphiql/graphiql.min.js"></script>
  <script>
    const fetcher = GraphiQL.createFetcher({ url: '/graphql' });
    ReactDOM.render(
      React.createElement(GraphiQL, { fetcher, defaultEditorToolsVisibility: true }),
      document.getElementById('graphiql')
    );
  </script>
</body>
</html>`

func servePlayground(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Write([]byte(playgroundHTML)) //nolint:errcheck
}
