// Package receiver implements the OTLP TraceService gRPC endpoint.
//
// The receiver sits between the client library and the downstream OTel
// Collector. For each batch it:
//  1. Calls the interceptor to enrich spans (resolve parent links, write DB).
//  2. Forwards the enriched batch to the downstream collector.
//  3. Returns the downstream response so the OTel SDK's built-in retry
//     logic handles transient failures correctly.
package receiver

import (
	"context"
	"fmt"

	collectortracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/HelixObs/herald/internal/interceptor"
)

// Receiver implements collectortracepb.TraceServiceServer.
type Receiver struct {
	collectortracepb.UnimplementedTraceServiceServer
	interceptor   *interceptor.Interceptor
	forwardClient collectortracepb.TraceServiceClient
}

// New returns a Receiver that uses client to forward enriched batches.
// Use this in tests to inject a mock client.
func New(icp *interceptor.Interceptor, client collectortracepb.TraceServiceClient) *Receiver {
	return &Receiver{interceptor: icp, forwardClient: client}
}

// Dial dials collectorEndpoint and returns a production Receiver.
func Dial(icp *interceptor.Interceptor, collectorEndpoint string) (*Receiver, error) {
	conn, err := grpc.NewClient(collectorEndpoint,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, fmt.Errorf("dial collector %s: %w", collectorEndpoint, err)
	}
	return New(icp, collectortracepb.NewTraceServiceClient(conn)), nil
}

// Export implements the OTLP TraceService. It enriches the batch in-place
// then forwards it to the downstream collector.
func (r *Receiver) Export(
	ctx context.Context,
	req *collectortracepb.ExportTraceServiceRequest,
) (*collectortracepb.ExportTraceServiceResponse, error) {
	r.interceptor.Process(req)
	return r.forwardClient.Export(ctx, req)
}
