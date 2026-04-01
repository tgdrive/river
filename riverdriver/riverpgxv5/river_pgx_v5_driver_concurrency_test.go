package riverpgxv5_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/internal/rivercommon"
	"github.com/riverqueue/river/riverdbtest"
	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivershared/riversharedtest"
	"github.com/riverqueue/river/rivershared/testfactory"
	"github.com/riverqueue/river/rivertype"
)

func TestExecutor_JobGetAvailableLimited_ByArgs(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	driver := riverpgxv5.New(riversharedtest.DBPool(ctx, t))
	tx, schema := riverdbtest.TestTxPgxDriver(ctx, t, driver, nil)
	exec := driver.UnwrapExecutor(tx)

	runningState := rivertype.JobStateRunning
	availableState := rivertype.JobStateAvailable

	runningSamePartition := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{
		EncodedArgs: []byte(`{"customer_id":1,"trace_id":"a"}`),
		Schema:      schema,
		State:       &runningState,
	})
	_ = runningSamePartition
	availableSamePartition := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{
		EncodedArgs: []byte(`{"customer_id":1,"trace_id":"b"}`),
		Schema:      schema,
		State:       &availableState,
	})
	availableOtherPartition := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{
		EncodedArgs: []byte(`{"customer_id":2,"trace_id":"c"}`),
		Schema:      schema,
		State:       &availableState,
	})

	jobs, err := exec.JobGetAvailableLimited(ctx, &riverdriver.JobGetAvailableLimitedParams{
		ClientID:        "client_1",
		GlobalLimit:     1,
		LocalLimit:      1,
		MaxAttemptedBy:  10,
		MaxToLock:       10,
		PartitionByArgs: []string{"customer_id"},
		Queue:           rivercommon.QueueDefault,
		Schema:          schema,
	})
	require.NoError(t, err)
	require.Len(t, jobs, 1)
	require.Equal(t, availableOtherPartition.ID, jobs[0].ID)
	require.NotEqual(t, availableSamePartition.ID, jobs[0].ID)
}

func TestExecutor_JobGetAvailableLimited_ByArgsEmptySliceUsesAllArgs(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	driver := riverpgxv5.New(riversharedtest.DBPool(ctx, t))
	tx, schema := riverdbtest.TestTxPgxDriver(ctx, t, driver, nil)
	exec := driver.UnwrapExecutor(tx)

	runningState := rivertype.JobStateRunning
	availableState := rivertype.JobStateAvailable

	testfactory.Job(ctx, t, exec, &testfactory.JobOpts{
		EncodedArgs: []byte(`{"customer_id":1,"trace_id":"a"}`),
		Schema:      schema,
		State:       &runningState,
	})
	blockedSameArgs := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{
		EncodedArgs: []byte(`{"customer_id":1,"trace_id":"a"}`),
		Schema:      schema,
		State:       &availableState,
	})
	allowedDifferentArgs := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{
		EncodedArgs: []byte(`{"customer_id":1,"trace_id":"b"}`),
		Schema:      schema,
		State:       &availableState,
	})

	jobs, err := exec.JobGetAvailableLimited(ctx, &riverdriver.JobGetAvailableLimitedParams{
		ClientID:        "client_1",
		GlobalLimit:     1,
		LocalLimit:      1,
		MaxAttemptedBy:  10,
		MaxToLock:       10,
		PartitionByArgs: []string{},
		Queue:           rivercommon.QueueDefault,
		Schema:          schema,
	})
	require.NoError(t, err)
	require.Len(t, jobs, 1)
	require.Equal(t, allowedDifferentArgs.ID, jobs[0].ID)
	require.NotEqual(t, blockedSameArgs.ID, jobs[0].ID)
}

func TestExecutor_JobGetAvailableLimited_NoPartitionHonorsLimit(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	driver := riverpgxv5.New(riversharedtest.DBPool(ctx, t))
	tx, schema := riverdbtest.TestTxPgxDriver(ctx, t, driver, nil)
	exec := driver.UnwrapExecutor(tx)

	runningState := rivertype.JobStateRunning
	availableState := rivertype.JobStateAvailable

	testfactory.Job(ctx, t, exec, &testfactory.JobOpts{Schema: schema, State: &runningState})
	available := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{Schema: schema, State: &availableState})

	jobs, err := exec.JobGetAvailableLimited(ctx, &riverdriver.JobGetAvailableLimitedParams{
		ClientID:       "client_1",
		GlobalLimit:    1,
		MaxAttemptedBy: 10,
		MaxToLock:      10,
		Queue:          rivercommon.QueueDefault,
		Schema:         schema,
	})
	require.NoError(t, err)
	require.Empty(t, jobs)

	job, err := exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: available.ID, Schema: schema})
	require.NoError(t, err)
	require.Equal(t, rivertype.JobStateAvailable, job.State)

}
