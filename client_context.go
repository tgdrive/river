package river

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/riverqueue/river/internal/rivercommon"
	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/rivertype"
)

var errClientNotInContext = errors.New("river: client not found in context, can only be used in a Worker")

type BatchClient interface {
	FetchBatch(ctx context.Context, leader *rivertype.JobRow, max int) ([]*rivertype.JobRow, error)
}

type contextKeyBatchClient struct{}

func withClient[TTx any](ctx context.Context, client *Client[TTx]) context.Context {
	ctx = context.WithValue(ctx, rivercommon.ContextKeyClient{}, client)
	if client != nil {
		ctx = context.WithValue(ctx, contextKeyBatchClient{}, batchClientAdapter[TTx]{client: client})
	}
	return ctx
}

// ClientFromContext returns the Client from the context. This function can
// only be used within a Worker's Work() method because that is the only place
// River sets the Client on the context.
//
// It panics if the context does not contain a Client, which will never happen
// from the context provided to a Worker's Work() method.
//
// When testing JobArgs.Work implementations, it might be useful to use
// rivertest.WorkContext to initialize a context that has an available client.
//
// The type parameter TTx is the transaction type used by the [Client],
// pgx.Tx for the pgx driver, and *sql.Tx for the [database/sql] driver.
func ClientFromContext[TTx any](ctx context.Context) *Client[TTx] {
	client, err := ClientFromContextSafely[TTx](ctx)
	if err != nil {
		panic(err)
	}
	return client
}

// ClientFromContextSafely returns the Client from the context. This function
// can only be used within a Worker's Work() method because that is the only
// place River sets the Client on the context.
//
// It returns an error if the context does not contain a Client, which will
// never happen from the context provided to a Worker's Work() method.
//
// When testing JobArgs.Work implementations, it might be useful to use
// rivertest.WorkContext to initialize a context that has an available client.
//
// See the examples for [ClientFromContext] to understand how to use this
// function.
func ClientFromContextSafely[TTx any](ctx context.Context) (*Client[TTx], error) {
	client, exists := ctx.Value(rivercommon.ContextKeyClient{}).(*Client[TTx])
	if !exists || client == nil {
		return nil, errClientNotInContext
	}
	return client, nil
}

// BatchClientFromContext returns a context-scoped batch client.
func BatchClientFromContext(ctx context.Context) BatchClient {
	client, err := BatchClientFromContextSafely(ctx)
	if err != nil {
		panic(err)
	}
	return client
}

// BatchClientFromContextSafely returns a context-scoped batch client.
func BatchClientFromContextSafely(ctx context.Context) (BatchClient, error) {
	client, ok := ctx.Value(contextKeyBatchClient{}).(BatchClient)
	if !ok || client == nil {
		return nil, errClientNotInContext
	}
	return client, nil
}

// ContextWithClient returns a context carrying the provided Client.
func ContextWithClient[TTx any](ctx context.Context, client *Client[TTx]) context.Context {
	return withClient(ctx, client)
}

type batchClientAdapter[TTx any] struct{ client *Client[TTx] }

func (a batchClientAdapter[TTx]) FetchBatch(ctx context.Context, leader *rivertype.JobRow, max int) ([]*rivertype.JobRow, error) {
	if leader == nil {
		return nil, errors.New("leader job cannot be nil")
	}

	batchKey := leader.Kind
	if len(leader.Metadata) > 0 {
		metadata := map[string]json.RawMessage{}
		if err := json.Unmarshal(leader.Metadata, &metadata); err == nil {
			if raw, ok := metadata["batch_key"]; ok {
				_ = json.Unmarshal(raw, &batchKey)
			}
		}
	}

	now := ptrTime(time.Now().UTC())
	return a.client.driver.GetExecutor().JobGetAvailableForBatch(ctx, &riverdriver.JobGetAvailableForBatchParams{
		BatchKey:       batchKey,
		ClientID:       a.client.config.ID,
		Kind:           leader.Kind,
		MaxAttemptedBy: 100,
		MaxToLock:      max,
		Now:            now,
		Queue:          leader.Queue,
		Schema:         a.client.config.Schema,
	})
}

func ptrTime(t time.Time) *time.Time { return &t }
