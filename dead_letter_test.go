package river

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/riverdbtest"
	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivershared/riversharedtest"
	"github.com/riverqueue/river/rivertype"
)

type deadLetterArgs struct{}

func (deadLetterArgs) Kind() string { return "dead_letter_args" }

func setupDeadLetterClient(ctx context.Context, t *testing.T) *Client[pgx.Tx] {
	t.Helper()

	dbPool := riversharedtest.DBPool(ctx, t)
	driver := riverpgxv5.New(dbPool)
	schema := riverdbtest.TestSchema(ctx, t, driver, nil)

	client, err := NewClient[pgx.Tx](driver, newTestConfig(t, schema))
	require.NoError(t, err)

	return client
}

func insertDiscardedJob(ctx context.Context, t *testing.T, client *Client[pgx.Tx]) *rivertype.JobRow {
	t.Helper()

	now := time.Now().UTC()
	job, err := client.driver.GetExecutor().JobInsertFull(ctx, &riverdriver.JobInsertFullParams{
		Attempt:     1,
		CreatedAt:   &now,
		EncodedArgs: []byte(`{"ok":true}`),
		Errors:      [][]byte{[]byte(`{"error":"boom"}`)},
		FinalizedAt: &now,
		Kind:        deadLetterArgs{}.Kind(),
		MaxAttempts: 5,
		Metadata:    []byte("{}"),
		Priority:    1,
		Queue:       QueueDefault,
		ScheduledAt: &now,
		Schema:      client.config.Schema,
		State:       rivertype.JobStateDiscarded,
	})
	require.NoError(t, err)
	return job
}

func moveDiscardedToDeadLetter(ctx context.Context, t *testing.T, client *Client[pgx.Tx]) {
	t.Helper()

	_, err := client.driver.GetExecutor().JobDeadLetterMoveDiscarded(ctx, &riverdriver.JobDeadLetterMoveDiscardedParams{
		DiscardedFinalizedAtHorizon: time.Now().UTC().Add(1 * time.Hour),
		Max:                         10,
		Schema:                      client.config.Schema,
	})
	require.NoError(t, err)
}

func TestClient_DeadLetterAPIs(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	client := setupDeadLetterClient(ctx, t)
	inserted := insertDiscardedJob(ctx, t, client)
	moveDiscardedToDeadLetter(ctx, t, client)

	jobs, err := client.JobDeadLetterList(ctx, 10)
	require.NoError(t, err)
	require.Len(t, jobs, 1)
	require.Equal(t, inserted.ID, jobs[0].ID)

	got, err := client.JobDeadLetterGet(ctx, inserted.ID)
	require.NoError(t, err)
	require.Equal(t, inserted.ID, got.ID)

	retried, err := client.JobDeadLetterRetry(ctx, inserted.ID)
	require.NoError(t, err)
	require.NotNil(t, retried)
	require.NotNil(t, retried.Job)
	require.Equal(t, rivertype.JobStateAvailable, retried.Job.State)
	require.Equal(t, 0, retried.Job.Attempt)
	require.Empty(t, retried.Job.Errors)

	jobsAfterRetry, err := client.JobDeadLetterList(ctx, 10)
	require.NoError(t, err)
	require.Empty(t, jobsAfterRetry)

	inserted2 := insertDiscardedJob(ctx, t, client)
	moveDiscardedToDeadLetter(ctx, t, client)

	deleted, err := client.JobDeadLetterDelete(ctx, inserted2.ID)
	require.NoError(t, err)
	require.Equal(t, inserted2.ID, deleted.ID)
}
