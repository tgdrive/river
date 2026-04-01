package river

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/riverdbtest"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivershared/riversharedtest"
	"github.com/riverqueue/river/rivertype"
)

type workflowCompatCancelArgs struct{}

func (workflowCompatCancelArgs) Kind() string { return "workflow_compat_cancel" }

type workflowCompatCompleteArgs struct{}

func (workflowCompatCompleteArgs) Kind() string { return "workflow_compat_complete" }

type workflowCompatFailArgs struct{}

func (workflowCompatFailArgs) Kind() string { return "workflow_compat_fail" }

type workflowCompatCancelWorker struct {
	WorkerDefaults[workflowCompatCancelArgs]
}

func (*workflowCompatCancelWorker) Work(context.Context, *Job[workflowCompatCancelArgs]) error {
	return JobCancel(nil)
}

type workflowCompatCompleteWorker struct {
	WorkerDefaults[workflowCompatCompleteArgs]
}

func (*workflowCompatCompleteWorker) Work(context.Context, *Job[workflowCompatCompleteArgs]) error {
	return nil
}

type workflowCompatFailWorker struct {
	WorkerDefaults[workflowCompatFailArgs]
}

func (*workflowCompatFailWorker) Work(context.Context, *Job[workflowCompatFailArgs]) error {
	return errors.New("boom")
}

func setupWorkflowCompatClient(ctx context.Context, t *testing.T, workers *Workers) (*Client[pgx.Tx], *riverpgxv5.Driver) {
	t.Helper()

	dbPool := riversharedtest.DBPool(ctx, t)
	driver := riverpgxv5.New(dbPool)
	schema := riverdbtest.TestSchema(ctx, t, driver, nil)

	config := newTestConfig(t, schema)
	config.Workers = workers

	client, err := NewClient[pgx.Tx](driver, config)
	require.NoError(t, err)

	return client, driver
}

func waitForWorkflowTaskStates(t *testing.T, workflow *WorkflowT[pgx.Tx], want map[string]rivertype.JobState) {
	t.Helper()

	require.Eventually(t, func() bool {
		for taskName, wantState := range want {
			task, err := workflow.LoadTask(context.Background(), taskName)
			if err != nil || task == nil || task.Job == nil || task.Job.State != wantState {
				return false
			}
		}
		return true
	}, 10*time.Second, 50*time.Millisecond)
}

func TestWorkflow_FailedDependencyCancelsPendingJobs_Compat(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	workers := NewWorkers()
	AddWorker(workers, &workflowCompatCancelWorker{})
	AddWorker(workers, &workflowCompatCompleteWorker{})

	client, _ := setupWorkflowCompatClient(ctx, t, workers)
	require.NoError(t, client.Start(ctx))
	t.Cleanup(func() { _ = client.Stop(context.Background()) })

	workflow := client.NewWorkflow(&WorkflowOpts{ID: "wf_failed_dep_cancel"})
	dep := workflow.Add("dep", workflowCompatCancelArgs{}, nil, nil)
	workflow.Add("child", workflowCompatCompleteArgs{}, nil, &WorkflowTaskOpts{Deps: []string{dep.Name}})

	prepared, err := workflow.Prepare(ctx)
	require.NoError(t, err)
	_, err = client.InsertMany(ctx, prepared.Jobs)
	require.NoError(t, err)

	waitForWorkflowTaskStates(t, workflow, map[string]rivertype.JobState{
		"dep":   rivertype.JobStateCancelled,
		"child": rivertype.JobStateCancelled,
	})
}

func TestWorkflow_IgnoreCancelledDependency_Compat(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	workers := NewWorkers()
	AddWorker(workers, &workflowCompatCancelWorker{})
	AddWorker(workers, &workflowCompatCompleteWorker{})

	client, _ := setupWorkflowCompatClient(ctx, t, workers)
	require.NoError(t, client.Start(ctx))
	t.Cleanup(func() { _ = client.Stop(context.Background()) })

	workflow := client.NewWorkflow(&WorkflowOpts{ID: "wf_ignore_cancelled", IgnoreCancelledDeps: true})
	dep := workflow.Add("dep", workflowCompatCancelArgs{}, nil, nil)
	workflow.Add("child", workflowCompatCompleteArgs{}, nil, &WorkflowTaskOpts{Deps: []string{dep.Name}})

	prepared, err := workflow.Prepare(ctx)
	require.NoError(t, err)
	_, err = client.InsertMany(ctx, prepared.Jobs)
	require.NoError(t, err)

	waitForWorkflowTaskStates(t, workflow, map[string]rivertype.JobState{
		"dep":   rivertype.JobStateCancelled,
		"child": rivertype.JobStateCompleted,
	})
}

func TestWorkflow_LoadTaskAndRetry_Compat(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	workers := NewWorkers()
	AddWorker(workers, &workflowCompatCompleteWorker{})
	AddWorker(workers, &workflowCompatFailWorker{})

	client, _ := setupWorkflowCompatClient(ctx, t, workers)
	require.NoError(t, client.Start(ctx))
	t.Cleanup(func() { _ = client.Stop(context.Background()) })

	workflow := client.NewWorkflow(&WorkflowOpts{ID: "wf_retry_failed_only"})
	root := workflow.Add("root", workflowCompatCompleteArgs{}, nil, nil)
	failed := workflow.Add("failed", workflowCompatFailArgs{}, &InsertOpts{MaxAttempts: 1}, &WorkflowTaskOpts{Deps: []string{root.Name}})
	workflow.Add("downstream", workflowCompatCompleteArgs{}, nil, &WorkflowTaskOpts{Deps: []string{failed.Name}})

	prepared, err := workflow.Prepare(ctx)
	require.NoError(t, err)
	_, err = client.InsertMany(ctx, prepared.Jobs)
	require.NoError(t, err)

	waitForWorkflowTaskStates(t, workflow, map[string]rivertype.JobState{
		"root":       rivertype.JobStateCompleted,
		"failed":     rivertype.JobStateDiscarded,
		"downstream": rivertype.JobStateCancelled,
	})

	loadedFailed, err := workflow.LoadTask(ctx, "failed")
	require.NoError(t, err)
	require.Equal(t, rivertype.JobStateDiscarded, loadedFailed.Job.State)

	_, err = workflow.LoadTask(ctx, "missing")
	require.ErrorIs(t, err, rivertype.ErrNotFound)

	require.NoError(t, client.Stop(ctx))

	retried, err := workflow.Retry(ctx, &WorkflowRetryOpts{Mode: WorkflowRetryModeFailedOnly, ResetHistory: true})
	require.NoError(t, err)
	require.Len(t, retried.Jobs, 2)

	rootTask, err := workflow.LoadTask(ctx, "root")
	require.NoError(t, err)
	require.Equal(t, rivertype.JobStateCompleted, rootTask.Job.State)

	failedTask, err := workflow.LoadTask(ctx, "failed")
	require.NoError(t, err)
	require.Equal(t, rivertype.JobStateAvailable, failedTask.Job.State)
	require.Equal(t, 0, failedTask.Job.Attempt)
	require.Empty(t, failedTask.Job.Errors)
	require.Nil(t, failedTask.Job.FinalizedAt)

	downstreamTask, err := workflow.LoadTask(ctx, "downstream")
	require.NoError(t, err)
	require.Equal(t, rivertype.JobStatePending, downstreamTask.Job.State)
}

func TestWorkflow_RetryStillActive_Compat(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	workers := NewWorkers()
	AddWorker(workers, &workflowCompatCompleteWorker{})

	client, _ := setupWorkflowCompatClient(ctx, t, workers)
	require.NoError(t, client.Start(ctx))
	t.Cleanup(func() { _ = client.Stop(context.Background()) })

	workflow := client.NewWorkflow(&WorkflowOpts{ID: "wf_retry_still_active"})
	workflow.Add("root", workflowCompatCompleteArgs{}, nil, nil)
	prepared, err := workflow.Prepare(ctx)
	require.NoError(t, err)
	_, err = client.InsertMany(ctx, prepared.Jobs)
	require.NoError(t, err)

	_, err = workflow.Retry(ctx, nil)
	require.Error(t, err)
	var retryErr *WorkflowRetryStillActiveError
	require.ErrorAs(t, err, &retryErr)
	require.Equal(t, workflow.ID(), retryErr.WorkflowID)
}

func TestWorkflow_RetryFailedOnly_DoesNotRetryManualCancelled_Compat(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	workers := NewWorkers()
	AddWorker(workers, &workflowCompatCompleteWorker{})
	AddWorker(workers, &workflowCompatFailWorker{})

	client, _ := setupWorkflowCompatClient(ctx, t, workers)

	workflow := client.NewWorkflow(&WorkflowOpts{ID: "wf_retry_manual_cancel_distinction"})
	root := workflow.Add("root", workflowCompatCompleteArgs{}, nil, nil)
	failed := workflow.Add("failed", workflowCompatFailArgs{}, &InsertOpts{MaxAttempts: 1}, &WorkflowTaskOpts{Deps: []string{root.Name}})
	workflow.Add("downstream", workflowCompatCompleteArgs{}, nil, &WorkflowTaskOpts{Deps: []string{failed.Name}})
	workflow.Add("manual", workflowCompatCompleteArgs{}, nil, nil)

	prepared, err := workflow.Prepare(ctx)
	require.NoError(t, err)
	_, err = client.InsertMany(ctx, prepared.Jobs)
	require.NoError(t, err)

	manualTask, err := workflow.LoadTask(ctx, "manual")
	require.NoError(t, err)
	_, err = client.JobCancel(ctx, manualTask.Job.ID)
	require.NoError(t, err)

	require.NoError(t, client.Start(ctx))
	t.Cleanup(func() { _ = client.Stop(context.Background()) })

	waitForWorkflowTaskStates(t, workflow, map[string]rivertype.JobState{
		"root":       rivertype.JobStateCompleted,
		"failed":     rivertype.JobStateDiscarded,
		"downstream": rivertype.JobStateCancelled,
		"manual":     rivertype.JobStateCancelled,
	})

	manualTask, err = workflow.LoadTask(ctx, "manual")
	require.NoError(t, err)
	manualMetadata := map[string]json.RawMessage{}
	require.NoError(t, json.Unmarshal(manualTask.Job.Metadata, &manualMetadata))
	_, hasFailedDepsMarker := manualMetadata["workflow_deps_failed_at"]
	require.False(t, hasFailedDepsMarker)

	require.NoError(t, client.Stop(ctx))

	retried, err := workflow.Retry(ctx, &WorkflowRetryOpts{Mode: WorkflowRetryModeFailedOnly})
	require.NoError(t, err)
	require.Len(t, retried.Jobs, 2)

	manualTask, err = workflow.LoadTask(ctx, "manual")
	require.NoError(t, err)
	require.Equal(t, rivertype.JobStateCancelled, manualTask.Job.State)

	downstreamTask, err := workflow.LoadTask(ctx, "downstream")
	require.NoError(t, err)
	require.Equal(t, rivertype.JobStatePending, downstreamTask.Job.State)
	downstreamMetadata := map[string]json.RawMessage{}
	require.NoError(t, json.Unmarshal(downstreamTask.Job.Metadata, &downstreamMetadata))
	_, hasFailedDepsMarker = downstreamMetadata["workflow_deps_failed_at"]
	require.True(t, hasFailedDepsMarker)
}

func TestWorkflow_RetryResetHistoryFalse_PreservesHistory_Compat(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	workers := NewWorkers()
	AddWorker(workers, &workflowCompatCompleteWorker{})
	AddWorker(workers, &workflowCompatFailWorker{})

	client, _ := setupWorkflowCompatClient(ctx, t, workers)
	require.NoError(t, client.Start(ctx))
	t.Cleanup(func() { _ = client.Stop(context.Background()) })

	workflow := client.NewWorkflow(&WorkflowOpts{ID: "wf_retry_preserve_history"})
	workflow.Add("failed", workflowCompatFailArgs{}, &InsertOpts{MaxAttempts: 1}, nil)

	prepared, err := workflow.Prepare(ctx)
	require.NoError(t, err)
	_, err = client.InsertMany(ctx, prepared.Jobs)
	require.NoError(t, err)

	waitForWorkflowTaskStates(t, workflow, map[string]rivertype.JobState{
		"failed": rivertype.JobStateDiscarded,
	})

	failedTask, err := workflow.LoadTask(ctx, "failed")
	require.NoError(t, err)
	oldAttempt := failedTask.Job.Attempt
	oldErrorsLen := len(failedTask.Job.Errors)
	require.NotZero(t, oldAttempt)
	require.NotZero(t, oldErrorsLen)

	require.NoError(t, client.Stop(ctx))

	_, err = workflow.Retry(ctx, &WorkflowRetryOpts{Mode: WorkflowRetryModeFailedOnly, ResetHistory: false})
	require.NoError(t, err)

	failedTask, err = workflow.LoadTask(ctx, "failed")
	require.NoError(t, err)
	require.Equal(t, rivertype.JobStateAvailable, failedTask.Job.State)
	require.Equal(t, oldAttempt, failedTask.Job.Attempt)
	require.Len(t, failedTask.Job.Errors, oldErrorsLen)
	require.Nil(t, failedTask.Job.FinalizedAt)
}
