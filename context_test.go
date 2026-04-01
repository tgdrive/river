package river

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

func TestClientFromContext(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	client := &Client[pgx.Tx]{}
	ctx = withClient(ctx, client)

	require.Equal(t, client, ClientFromContext[pgx.Tx](ctx))

	result, err := ClientFromContextSafely[pgx.Tx](ctx)
	require.NoError(t, err)
	require.Equal(t, client, result)

	require.PanicsWithError(t, errClientNotInContext.Error(), func() {
		ClientFromContext[pgx.Tx](context.Background())
	})

	result, err = ClientFromContextSafely[pgx.Tx](context.Background())
	require.ErrorIs(t, err, errClientNotInContext)
	require.Nil(t, result)
}

func TestContextWithClient(t *testing.T) {
	t.Parallel()

	ctx := ContextWithClient(context.Background(), &Client[pgx.Tx]{})

	client, err := ClientFromContextSafely[pgx.Tx](ctx)
	require.NoError(t, err)
	require.NotNil(t, client)

	batchClient, err := BatchClientFromContextSafely(ctx)
	require.NoError(t, err)
	require.NotNil(t, batchClient)
}

func TestBatchClientFromContext(t *testing.T) {
	t.Parallel()

	require.PanicsWithError(t, errClientNotInContext.Error(), func() {
		BatchClientFromContext(context.Background())
	})

	batchClient, err := BatchClientFromContextSafely(context.Background())
	require.ErrorIs(t, err, errClientNotInContext)
	require.Nil(t, batchClient)
}
