package river

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/riverdbtest"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivershared/riversharedtest"
	"github.com/riverqueue/river/rivertype"
)

type ephemeralFailArgs struct{ Mode string }

func (ephemeralFailArgs) Kind() string                 { return "ephemeral_fail" }
func (ephemeralFailArgs) EphemeralOpts() EphemeralOpts { return EphemeralOpts{} }

type ephemeralFailWorker struct {
	WorkerDefaults[ephemeralFailArgs]
}

func (*ephemeralFailWorker) Work(_ context.Context, job *Job[ephemeralFailArgs]) error {
	switch job.Args.Mode {
	case "cancel":
		return JobCancel(nil)
	default:
		return errors.New("ephemeral failure")
	}
}

func setupEphemeralCompatClient(ctx context.Context, t *testing.T) *Client[pgx.Tx] {
	t.Helper()
	dbPool := riversharedtest.DBPool(ctx, t)
	driver := riverpgxv5.New(dbPool)
	schema := riverdbtest.TestSchema(ctx, t, driver, nil)
	workers := NewWorkers()
	AddWorker(workers, &ephemeralFailWorker{})
	client, err := NewClient[pgx.Tx](driver, &Config{
		Queues:   map[string]QueueConfig{QueueDefault: {MaxWorkers: 10}},
		Schema:   schema,
		TestOnly: true,
		Workers:  workers,
	})
	require.NoError(t, err)
	return client
}

func TestEphemeralJob_FailedRetryPathIsNotDeleted_Compat(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	client := setupEphemeralCompatClient(ctx, t)
	failed, cancel := client.Subscribe(EventKindJobFailed)
	defer cancel()

	inserted, err := client.Insert(ctx, ephemeralFailArgs{Mode: "retryable"}, &InsertOpts{MaxAttempts: 2})
	require.NoError(t, err)

	require.NoError(t, client.Start(ctx))
	riversharedtest.WaitOrTimeoutN(t, failed, 1)
	require.NoError(t, client.Stop(ctx))

	job, err := client.JobGet(ctx, inserted.Job.ID)
	require.NoError(t, err)
	require.Contains(t, []rivertype.JobState{rivertype.JobStateAvailable, rivertype.JobStateRetryable}, job.State)
}

func TestEphemeralJob_DiscardedIsNotDeleted_Compat(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	client := setupEphemeralCompatClient(ctx, t)
	failed, cancel := client.Subscribe(EventKindJobFailed)
	defer cancel()

	inserted, err := client.Insert(ctx, ephemeralFailArgs{Mode: "discarded"}, &InsertOpts{MaxAttempts: 1})
	require.NoError(t, err)

	require.NoError(t, client.Start(ctx))
	riversharedtest.WaitOrTimeoutN(t, failed, 1)
	require.NoError(t, client.Stop(ctx))

	job, err := client.JobGet(ctx, inserted.Job.ID)
	require.NoError(t, err)
	require.Equal(t, rivertype.JobStateDiscarded, job.State)
}

func TestEphemeralJob_CancelledIsNotDeleted_Compat(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	client := setupEphemeralCompatClient(ctx, t)
	cancelled, cancel := client.Subscribe(EventKindJobCancelled)
	defer cancel()

	inserted, err := client.Insert(ctx, ephemeralFailArgs{Mode: "cancel"}, nil)
	require.NoError(t, err)

	require.NoError(t, client.Start(ctx))
	riversharedtest.WaitOrTimeoutN(t, cancelled, 1)
	require.NoError(t, client.Stop(ctx))

	job, err := client.JobGet(ctx, inserted.Job.ID)
	require.NoError(t, err)
	require.Equal(t, rivertype.JobStateCancelled, job.State)
}
