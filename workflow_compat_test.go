package river

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
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

type workflowCompatRuntimeArgs struct {
	AddTaskName string    `json:"add_task_name,omitempty"`
	Behavior    string    `json:"behavior,omitempty"`
	ScheduledAt time.Time `json:"scheduled_at,omitempty"`
	TaskName    string    `json:"task_name"`
	WorkflowID  string    `json:"workflow_id,omitempty"`
}

func (workflowCompatRuntimeArgs) Kind() string { return "workflow_compat_runtime" }

type workflowCompatRuntimeState struct {
	events       chan string
	mu           sync.Mutex
	releases     map[string]chan struct{}
	started      chan string
	startedCount map[string]*atomic.Int64
}

func newWorkflowCompatRuntimeState() *workflowCompatRuntimeState {
	return &workflowCompatRuntimeState{
		events:       make(chan string, 64),
		releases:     map[string]chan struct{}{},
		started:      make(chan string, 32),
		startedCount: map[string]*atomic.Int64{},
	}
}

func (s *workflowCompatRuntimeState) ensureCounter(taskName string) *atomic.Int64 {
	s.mu.Lock()
	defer s.mu.Unlock()

	if counter, ok := s.startedCount[taskName]; ok {
		return counter
	}

	counter := &atomic.Int64{}
	s.startedCount[taskName] = counter
	return counter
}

func (s *workflowCompatRuntimeState) hold(taskName string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.releases[taskName]; ok {
		return
	}

	s.releases[taskName] = make(chan struct{})
}

func (s *workflowCompatRuntimeState) release(taskName string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	releaseCh, ok := s.releases[taskName]
	if !ok {
		return
	}

	delete(s.releases, taskName)
	close(releaseCh)
}

func (s *workflowCompatRuntimeState) startedTimes(taskName string) int64 {
	return s.ensureCounter(taskName).Load()
}

func (s *workflowCompatRuntimeState) waitForStart(t *testing.T, wantTaskName string) {
	t.Helper()

	require.Eventually(t, func() bool {
		select {
		case gotTaskName := <-s.started:
			return gotTaskName == wantTaskName
		default:
			return false
		}
	}, 10*time.Second, 10*time.Millisecond)
}

func (s *workflowCompatRuntimeState) waitForStarts(t *testing.T, wantTaskNames ...string) {
	t.Helper()

	remaining := make(map[string]int, len(wantTaskNames))
	for _, taskName := range wantTaskNames {
		remaining[taskName]++
	}

	require.Eventually(t, func() bool {
		for {
			select {
			case taskName := <-s.started:
				if remaining[taskName] > 0 {
					remaining[taskName]--
					if remaining[taskName] == 0 {
						delete(remaining, taskName)
					}
				}
			default:
				return len(remaining) == 0
			}
		}
	}, 10*time.Second, 10*time.Millisecond)
}

func (s *workflowCompatRuntimeState) waitForEvent(t *testing.T, want string) {
	t.Helper()

	require.Eventually(t, func() bool {
		select {
		case got := <-s.events:
			return got == want
		default:
			return false
		}
	}, 10*time.Second, 10*time.Millisecond)
}

func (s *workflowCompatRuntimeState) waitForEvents(t *testing.T, wantEvents ...string) {
	t.Helper()

	remaining := make(map[string]int, len(wantEvents))
	for _, event := range wantEvents {
		remaining[event]++
	}

	require.Eventually(t, func() bool {
		for {
			select {
			case event := <-s.events:
				if remaining[event] > 0 {
					remaining[event]--
					if remaining[event] == 0 {
						delete(remaining, event)
					}
				}
			default:
				return len(remaining) == 0
			}
		}
	}, 10*time.Second, 10*time.Millisecond)
}

func (s *workflowCompatRuntimeState) waitForNoEvent(t *testing.T, want string, waitDuration time.Duration) {
	t.Helper()

	require.Never(t, func() bool {
		for {
			select {
			case got := <-s.events:
				if got == want {
					return true
				}
			default:
				return false
			}
		}
	}, waitDuration, 10*time.Millisecond)
}

type workflowCompatRuntimeWorker struct {
	WorkerDefaults[workflowCompatRuntimeArgs]

	client *Client[pgx.Tx]
	dbPool interface {
		Begin(ctx context.Context) (pgx.Tx, error)
	}
	state *workflowCompatRuntimeState
}

func (w *workflowCompatRuntimeWorker) Work(ctx context.Context, job *Job[workflowCompatRuntimeArgs]) error {
	args := job.Args
	w.state.ensureCounter(args.TaskName).Add(1)

	select {
	case w.state.started <- args.TaskName:
	default:
	}

	w.state.mu.Lock()
	releaseCh := w.state.releases[args.TaskName]
	w.state.mu.Unlock()

	if releaseCh != nil {
		select {
		case <-ctx.Done():
			return JobCancel(ctx.Err())
		case <-releaseCh:
		}
	}

	switch args.Behavior {
	case "append":
		tx, err := w.dbPool.Begin(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback(ctx)

		workflow, err := w.client.WorkflowFromExisting(job.JobRow, nil)
		if err != nil {
			return err
		}

		workflow.Add(args.AddTaskName, workflowCompatRuntimeArgs{Behavior: "complete", TaskName: args.AddTaskName, WorkflowID: args.WorkflowID}, nil, nil)

		prepared, err := workflow.PrepareTx(ctx, tx)
		if err != nil {
			return err
		}

		if _, err := w.client.InsertManyTx(ctx, tx, prepared.Jobs); err != nil {
			return err
		}

		if _, err := JobCompleteTx[*riverpgxv5.Driver](ctx, tx, job); err != nil {
			return err
		}

		return tx.Commit(ctx)
	case "cancel":
		return JobCancel(nil)
	case "fail":
		return errors.New("boom")
	default:
		return RecordOutput(ctx, map[string]any{
			"scheduled_at": args.ScheduledAt,
			"task_name":    args.TaskName,
		})
	}
}

func setupWorkflowCompatClient(ctx context.Context, t *testing.T, workers *Workers) *Client[pgx.Tx] {
	t.Helper()

	dbPool := riversharedtest.DBPool(ctx, t)
	driver := riverpgxv5.New(dbPool)
	schema := riverdbtest.TestSchema(ctx, t, driver, nil)

	config := newTestConfig(t, schema)
	config.Workers = workers

	client, err := NewClient[pgx.Tx](driver, config)
	require.NoError(t, err)

	return client
}

func setupWorkflowCompatClientWithRuntimeWorker(ctx context.Context, t *testing.T, queueConfig QueueConfig) (*Client[pgx.Tx], *workflowCompatRuntimeState) {
	t.Helper()

	dbPool := riversharedtest.DBPool(ctx, t)
	driver := riverpgxv5.New(dbPool)
	schema := riverdbtest.TestSchema(ctx, t, driver, nil)
	workers := NewWorkers()
	state := newWorkflowCompatRuntimeState()

	client, err := NewClient[pgx.Tx](driver, &Config{
		FetchCooldown:     10 * time.Millisecond,
		FetchPollInterval: 10 * time.Millisecond,
		Logger:            riversharedtest.Logger(t),
		MaxAttempts:       MaxAttemptsDefault,
		Queues:            map[string]QueueConfig{QueueDefault: queueConfig},
		Schema:            schema,
		Test: TestConfig{
			Time: &riversharedtest.TimeStub{},
		},
		TestOnly:          true,
		Workers:           workers,
		schedulerInterval: 10 * time.Millisecond,
	})
	require.NoError(t, err)

	AddWorker(workers, &workflowCompatRuntimeWorker{client: client, dbPool: dbPool, state: state})

	subscribeCh, cancel := client.Subscribe(EventKindJobCancelled, EventKindJobCompleted, EventKindJobFailed)
	t.Cleanup(cancel)

	go func() {
		for event := range subscribeCh {
			if event == nil || event.Job == nil {
				continue
			}

			taskName, err := workflowTaskNameFromJob(event.Job)
			if err != nil || taskName == "" {
				continue
			}

			label := string(event.Kind) + ":" + taskName
			select {
			case state.events <- label:
			default:
			}
		}
	}()

	return client, state
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

	client := setupWorkflowCompatClient(ctx, t, workers)
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

	client := setupWorkflowCompatClient(ctx, t, workers)
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

	client := setupWorkflowCompatClient(ctx, t, workers)
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

	client := setupWorkflowCompatClient(ctx, t, workers)
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

	client := setupWorkflowCompatClient(ctx, t, workers)

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

	client := setupWorkflowCompatClient(ctx, t, workers)
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

func TestWorkflow_Cancel_LoadAllAndLoadDeps_Compat(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	client, state := setupWorkflowCompatClientWithRuntimeWorker(ctx, t, QueueConfig{MaxWorkers: 4})
	state.hold("root")
	require.NoError(t, client.Start(ctx))
	t.Cleanup(func() { _ = client.Stop(context.Background()) })

	workflow := client.NewWorkflow(&WorkflowOpts{ID: "wf_cancel_load_all"})
	root := workflow.Add("root", workflowCompatRuntimeArgs{TaskName: "root"}, nil, nil)
	workflow.Add("child_a", workflowCompatRuntimeArgs{TaskName: "child_a"}, nil, &WorkflowTaskOpts{Deps: []string{root.Name}})
	workflow.Add("child_b", workflowCompatRuntimeArgs{TaskName: "child_b"}, nil, &WorkflowTaskOpts{Deps: []string{root.Name}})

	prepared, err := workflow.Prepare(ctx)
	require.NoError(t, err)
	_, err = client.InsertMany(ctx, prepared.Jobs)
	require.NoError(t, err)

	state.waitForStart(t, "root")

	all, err := workflow.LoadAll(ctx, nil)
	require.NoError(t, err)
	require.Equal(t, 3, all.Count())
	require.Equal(t, []string{"child_a", "child_b", "root"}, all.Names())
	require.Equal(t, rivertype.JobStateRunning, all.Get("root").Job.State)
	require.Equal(t, rivertype.JobStatePending, all.Get("child_a").Job.State)
	require.Equal(t, rivertype.JobStatePending, all.Get("child_b").Job.State)

	deps, err := workflow.LoadDeps(ctx, "child_a", &WorkflowLoadDepsOpts{Recursive: true})
	require.NoError(t, err)
	require.Equal(t, []string{"child_a", "root"}, deps.Names())

	result, err := client.WorkflowCancel(ctx, workflow.ID())
	require.NoError(t, err)
	require.Len(t, result.CancelledJobs, 3)
	state.waitForEvent(t, "job_cancelled:root")

	waitForWorkflowTaskStates(t, workflow, map[string]rivertype.JobState{
		"child_a": rivertype.JobStateCancelled,
		"child_b": rivertype.JobStateCancelled,
		"root":    rivertype.JobStateCancelled,
	})

	require.Zero(t, state.startedTimes("child_a"))
	require.Zero(t, state.startedTimes("child_b"))
}

func TestWorkflow_Cancel_CancelsRunningTasks_Compat(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	client, state := setupWorkflowCompatClientWithRuntimeWorker(ctx, t, QueueConfig{MaxWorkers: 4})
	state.hold("root")
	require.NoError(t, client.Start(ctx))
	t.Cleanup(func() { _ = client.Stop(context.Background()) })

	workflow := client.NewWorkflow(&WorkflowOpts{ID: "wf_cancel_cancels_running"})
	root := workflow.Add("root", workflowCompatRuntimeArgs{TaskName: "root"}, nil, nil)
	workflow.Add("child", workflowCompatRuntimeArgs{TaskName: "child"}, nil, &WorkflowTaskOpts{Deps: []string{root.Name}})

	prepared, err := workflow.Prepare(ctx)
	require.NoError(t, err)
	_, err = client.InsertMany(ctx, prepared.Jobs)
	require.NoError(t, err)

	state.waitForStart(t, "root")

	result, err := client.WorkflowCancel(ctx, workflow.ID())
	require.NoError(t, err)
	require.Len(t, result.CancelledJobs, 2)

	state.waitForEvent(t, "job_cancelled:root")

	waitForWorkflowTaskStates(t, workflow, map[string]rivertype.JobState{
		"child": rivertype.JobStateCancelled,
		"root":  rivertype.JobStateCancelled,
	})
}

func TestWorkflow_JobCancel_CancelsRunningTaskAndDownstream_Compat(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	client, state := setupWorkflowCompatClientWithRuntimeWorker(ctx, t, QueueConfig{MaxWorkers: 4})
	state.hold("root")
	require.NoError(t, client.Start(ctx))
	t.Cleanup(func() { _ = client.Stop(context.Background()) })

	workflow := client.NewWorkflow(&WorkflowOpts{ID: "wf_job_cancel_cancels_running"})
	root := workflow.Add("root", workflowCompatRuntimeArgs{TaskName: "root"}, nil, nil)
	workflow.Add("child", workflowCompatRuntimeArgs{TaskName: "child"}, nil, &WorkflowTaskOpts{Deps: []string{root.Name}})

	prepared, err := workflow.Prepare(ctx)
	require.NoError(t, err)
	_, err = client.InsertMany(ctx, prepared.Jobs)
	require.NoError(t, err)

	state.waitForStart(t, "root")

	rootTask, err := workflow.LoadTask(ctx, "root")
	require.NoError(t, err)

	_, err = client.JobCancel(ctx, rootTask.Job.ID)
	require.NoError(t, err)

	state.waitForEvent(t, "job_cancelled:root")

	waitForWorkflowTaskStates(t, workflow, map[string]rivertype.JobState{
		"child": rivertype.JobStateCancelled,
		"root":  rivertype.JobStateCancelled,
	})
}

func TestWorkflow_FromExisting_AddTasks_Compat(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	client, state := setupWorkflowCompatClientWithRuntimeWorker(ctx, t, QueueConfig{MaxWorkers: 4})
	require.NoError(t, client.Start(ctx))
	t.Cleanup(func() { _ = client.Stop(context.Background()) })

	workflow := client.NewWorkflow(&WorkflowOpts{ID: "wf_from_existing_add_tasks"})
	workflow.Add("seed", workflowCompatRuntimeArgs{AddTaskName: "added", Behavior: "append", TaskName: "seed", WorkflowID: workflow.ID()}, nil, nil)

	prepared, err := workflow.Prepare(ctx)
	require.NoError(t, err)
	_, err = client.InsertMany(ctx, prepared.Jobs)
	require.NoError(t, err)

	state.waitForStarts(t, "seed", "added")

	waitForWorkflowTaskStates(t, workflow, map[string]rivertype.JobState{
		"added": rivertype.JobStateCompleted,
		"seed":  rivertype.JobStateCompleted,
	})

	all, err := workflow.LoadAll(ctx, nil)
	require.NoError(t, err)
	require.Equal(t, []string{"added", "seed"}, all.Names())

	var output struct {
		TaskName string `json:"task_name"`
	}
	require.NoError(t, workflow.LoadOutput(ctx, "added", &output))
	require.Equal(t, "added", output.TaskName)
}

func TestWorkflow_MultiWorker_DownstreamWaitsForAllDeps_Compat(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	client, state := setupWorkflowCompatClientWithRuntimeWorker(ctx, t, QueueConfig{MaxWorkers: 4})
	state.hold("dep_a")
	state.hold("dep_b")
	require.NoError(t, client.Start(ctx))
	t.Cleanup(func() { _ = client.Stop(context.Background()) })

	workflow := client.NewWorkflow(&WorkflowOpts{ID: "wf_multi_waits_for_all"})
	workflow.Add("dep_a", workflowCompatRuntimeArgs{TaskName: "dep_a"}, nil, nil)
	workflow.Add("dep_b", workflowCompatRuntimeArgs{TaskName: "dep_b"}, nil, nil)
	workflow.Add("child", workflowCompatRuntimeArgs{TaskName: "child"}, nil, &WorkflowTaskOpts{Deps: []string{"dep_a", "dep_b"}})

	prepared, err := workflow.Prepare(ctx)
	require.NoError(t, err)
	_, err = client.InsertMany(ctx, prepared.Jobs)
	require.NoError(t, err)

	state.waitForStarts(t, "dep_a", "dep_b")

	childTask, err := workflow.LoadTask(ctx, "child")
	require.NoError(t, err)
	require.Equal(t, rivertype.JobStatePending, childTask.Job.State)
	require.Zero(t, state.startedTimes("child"))

	state.release("dep_a")
	waitForWorkflowTaskStates(t, workflow, map[string]rivertype.JobState{
		"dep_a": rivertype.JobStateCompleted,
		"dep_b": rivertype.JobStateRunning,
		"child": rivertype.JobStatePending,
	})
	require.Zero(t, state.startedTimes("child"))

	state.release("dep_b")
	state.waitForStart(t, "child")

	waitForWorkflowTaskStates(t, workflow, map[string]rivertype.JobState{
		"child": rivertype.JobStateCompleted,
		"dep_a": rivertype.JobStateCompleted,
		"dep_b": rivertype.JobStateCompleted,
	})

	require.GreaterOrEqual(t, state.startedTimes("dep_a")+state.startedTimes("dep_b"), int64(2))

	deps, err := workflow.LoadDeps(ctx, "child", &WorkflowLoadDepsOpts{Recursive: true})
	require.NoError(t, err)
	require.Equal(t, []string{"child", "dep_a", "dep_b"}, deps.Names())
}

func TestWorkflow_ScheduledTaskWaitsForDepsAndTime_Compat(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	client, state := setupWorkflowCompatClientWithRuntimeWorker(ctx, t, QueueConfig{MaxWorkers: 4})
	future := time.Now().UTC().Add(2 * time.Second).Round(time.Millisecond)
	require.NoError(t, client.Start(ctx))
	t.Cleanup(func() { _ = client.Stop(context.Background()) })

	workflow := client.NewWorkflow(&WorkflowOpts{ID: "wf_scheduled_waits_for_deps_and_time"})
	dep := workflow.Add("dep", workflowCompatRuntimeArgs{TaskName: "dep"}, nil, nil)
	workflow.Add("scheduled", workflowCompatRuntimeArgs{ScheduledAt: future, TaskName: "scheduled"}, &InsertOpts{ScheduledAt: future}, &WorkflowTaskOpts{Deps: []string{dep.Name}})

	prepared, err := workflow.Prepare(ctx)
	require.NoError(t, err)
	_, err = client.InsertMany(ctx, prepared.Jobs)
	require.NoError(t, err)

	state.waitForStart(t, "dep")
	waitForWorkflowTaskStates(t, workflow, map[string]rivertype.JobState{
		"dep": rivertype.JobStateCompleted,
	})

	require.Never(t, func() bool {
		return state.startedTimes("scheduled") > 0
	}, time.Until(future.Add(-250*time.Millisecond)), 25*time.Millisecond)

	require.Eventually(t, func() bool {
		return state.startedTimes("scheduled") > 0
	}, 5*time.Second, 25*time.Millisecond)

	waitForWorkflowTaskStates(t, workflow, map[string]rivertype.JobState{
		"scheduled": rivertype.JobStateCompleted,
	})

	task, err := workflow.LoadTask(ctx, "scheduled")
	require.NoError(t, err)
	require.NotNil(t, task.Job.AttemptedAt)
	require.False(t, task.Job.AttemptedAt.Before(future.Add(-150*time.Millisecond)), fmt.Sprintf("scheduled task attempted too early: got %s want >= %s", task.Job.AttemptedAt.UTC().Format(time.RFC3339Nano), future.Add(-150*time.Millisecond).Format(time.RFC3339Nano)))

	var output struct {
		ScheduledAt time.Time `json:"scheduled_at"`
		TaskName    string    `json:"task_name"`
	}
	require.NoError(t, workflow.LoadOutput(ctx, "scheduled", &output))
	require.Equal(t, "scheduled", output.TaskName)
	require.WithinDuration(t, future, output.ScheduledAt, time.Millisecond)
}

func TestWorkflow_LoadByJob_Compat(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	client, state := setupWorkflowCompatClientWithRuntimeWorker(ctx, t, QueueConfig{MaxWorkers: 4})
	require.NoError(t, client.Start(ctx))
	t.Cleanup(func() { _ = client.Stop(context.Background()) })

	workflow := client.NewWorkflow(&WorkflowOpts{ID: "wf_load_by_job"})
	dep := workflow.Add("dep", workflowCompatRuntimeArgs{TaskName: "dep"}, nil, nil)
	workflow.Add("child", workflowCompatRuntimeArgs{TaskName: "child"}, nil, &WorkflowTaskOpts{Deps: []string{dep.Name}})

	prepared, err := workflow.Prepare(ctx)
	require.NoError(t, err)
	inserted, err := client.InsertMany(ctx, prepared.Jobs)
	require.NoError(t, err)

	state.waitForEvents(t, "job_completed:dep", "job_completed:child")

	childTask, err := workflow.LoadTask(ctx, "child")
	require.NoError(t, err)

	deps, err := workflow.LoadDepsByJob(ctx, childTask.Job, &WorkflowLoadDepsOpts{Recursive: true})
	require.NoError(t, err)
	require.Equal(t, []string{"child", "dep"}, deps.Names())

	var output struct {
		TaskName string `json:"task_name"`
	}
	require.NoError(t, workflow.LoadOutputByJob(ctx, inserted[0].Job, &output))
	require.Equal(t, "dep", output.TaskName)
}

func TestWorkflow_MultiWorker_CancelledDepCancelsDownstreamWithoutStart_Compat(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	client, state := setupWorkflowCompatClientWithRuntimeWorker(ctx, t, QueueConfig{MaxWorkers: 4})
	state.hold("dep_running")
	require.NoError(t, client.Start(ctx))
	t.Cleanup(func() { _ = client.Stop(context.Background()) })

	workflow := client.NewWorkflow(&WorkflowOpts{ID: "wf_cancelled_dep_cancels_downstream"})
	workflow.Add("dep_cancel", workflowCompatRuntimeArgs{Behavior: "cancel", TaskName: "dep_cancel"}, nil, nil)
	workflow.Add("dep_running", workflowCompatRuntimeArgs{TaskName: "dep_running"}, nil, nil)
	workflow.Add("child", workflowCompatRuntimeArgs{TaskName: "child"}, nil, &WorkflowTaskOpts{Deps: []string{"dep_cancel", "dep_running"}})

	prepared, err := workflow.Prepare(ctx)
	require.NoError(t, err)
	_, err = client.InsertMany(ctx, prepared.Jobs)
	require.NoError(t, err)

	state.waitForStarts(t, "dep_cancel", "dep_running")
	state.waitForEvent(t, "job_cancelled:dep_cancel")

	require.Eventually(t, func() bool {
		childTask, err := workflow.LoadTask(ctx, "child")
		return err == nil && childTask.Job.State == rivertype.JobStateCancelled
	}, 10*time.Second, 25*time.Millisecond)

	require.Zero(t, state.startedTimes("child"))
	state.release("dep_running")
	waitForWorkflowTaskStates(t, workflow, map[string]rivertype.JobState{
		"dep_running": rivertype.JobStateCompleted,
	})
}

func TestWorkflow_MultiWorker_IgnoreCancelledDepWaitsForRemainingDeps_Compat(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	client, state := setupWorkflowCompatClientWithRuntimeWorker(ctx, t, QueueConfig{MaxWorkers: 4})
	state.hold("dep_running")
	require.NoError(t, client.Start(ctx))
	t.Cleanup(func() { _ = client.Stop(context.Background()) })

	workflow := client.NewWorkflow(&WorkflowOpts{ID: "wf_ignore_cancelled_waits_for_remaining"})
	workflow.Add("dep_cancel", workflowCompatRuntimeArgs{Behavior: "cancel", TaskName: "dep_cancel"}, nil, nil)
	workflow.Add("dep_running", workflowCompatRuntimeArgs{TaskName: "dep_running"}, nil, nil)
	workflow.Add("child", workflowCompatRuntimeArgs{TaskName: "child"}, nil, &WorkflowTaskOpts{Deps: []string{"dep_cancel", "dep_running"}, IgnoreCancelledDeps: true})

	prepared, err := workflow.Prepare(ctx)
	require.NoError(t, err)
	_, err = client.InsertMany(ctx, prepared.Jobs)
	require.NoError(t, err)

	state.waitForStarts(t, "dep_cancel", "dep_running")
	state.waitForEvent(t, "job_cancelled:dep_cancel")

	childTask, err := workflow.LoadTask(ctx, "child")
	require.NoError(t, err)
	require.Equal(t, rivertype.JobStatePending, childTask.Job.State)
	require.Zero(t, state.startedTimes("child"))
	state.waitForNoEvent(t, "job_cancelled:child", 250*time.Millisecond)

	state.release("dep_running")
	state.waitForStart(t, "child")
	state.waitForEvents(t, "job_completed:dep_running", "job_completed:child")

	waitForWorkflowTaskStates(t, workflow, map[string]rivertype.JobState{
		"child":       rivertype.JobStateCompleted,
		"dep_cancel":  rivertype.JobStateCancelled,
		"dep_running": rivertype.JobStateCompleted,
	})
}

func TestWorkflow_RetryFailedAndDownstream_Compat(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	client, state := setupWorkflowCompatClientWithRuntimeWorker(ctx, t, QueueConfig{MaxWorkers: 4})
	require.NoError(t, client.Start(ctx))
	t.Cleanup(func() { _ = client.Stop(context.Background()) })

	workflow := client.NewWorkflow(&WorkflowOpts{ID: "wf_retry_failed_and_downstream"})
	workflow.Add("root", workflowCompatRuntimeArgs{TaskName: "root"}, nil, nil)
	workflow.Add("failed", workflowCompatRuntimeArgs{Behavior: "fail", TaskName: "failed"}, &InsertOpts{MaxAttempts: 1}, &WorkflowTaskOpts{Deps: []string{"root"}})
	workflow.Add("downstream", workflowCompatRuntimeArgs{TaskName: "downstream"}, nil, &WorkflowTaskOpts{Deps: []string{"failed"}})
	workflow.Add("independent", workflowCompatRuntimeArgs{TaskName: "independent"}, nil, nil)

	prepared, err := workflow.Prepare(ctx)
	require.NoError(t, err)
	_, err = client.InsertMany(ctx, prepared.Jobs)
	require.NoError(t, err)

	state.waitForEvents(t, "job_completed:root", "job_failed:failed", "job_completed:independent")

	waitForWorkflowTaskStates(t, workflow, map[string]rivertype.JobState{
		"downstream":  rivertype.JobStateCancelled,
		"failed":      rivertype.JobStateDiscarded,
		"independent": rivertype.JobStateCompleted,
		"root":        rivertype.JobStateCompleted,
	})

	require.NoError(t, client.Stop(ctx))

	retried, err := workflow.Retry(ctx, &WorkflowRetryOpts{Mode: WorkflowRetryModeFailedAndDownstream, ResetHistory: true})
	require.NoError(t, err)
	require.Len(t, retried.Jobs, 2)

	rootTask, err := workflow.LoadTask(ctx, "root")
	require.NoError(t, err)
	require.Equal(t, rivertype.JobStateCompleted, rootTask.Job.State)

	independentTask, err := workflow.LoadTask(ctx, "independent")
	require.NoError(t, err)
	require.Equal(t, rivertype.JobStateCompleted, independentTask.Job.State)

	failedTask, err := workflow.LoadTask(ctx, "failed")
	require.NoError(t, err)
	require.Equal(t, rivertype.JobStateAvailable, failedTask.Job.State)

	downstreamTask, err := workflow.LoadTask(ctx, "downstream")
	require.NoError(t, err)
	require.Equal(t, rivertype.JobStatePending, downstreamTask.Job.State)

	require.NoError(t, client.Start(ctx))
	waitForWorkflowTaskStates(t, workflow, map[string]rivertype.JobState{
		"downstream": rivertype.JobStateCancelled,
		"failed":     rivertype.JobStateDiscarded,
	})
	require.Zero(t, state.startedTimes("downstream"))
}
