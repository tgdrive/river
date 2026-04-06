package river_test

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdbtest"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivershared/riversharedtest"
	"github.com/riverqueue/river/rivertype"
)

type durablePeriodicExampleArgs struct{}

func (durablePeriodicExampleArgs) Kind() string { return "durable_periodic_example" }

type durablePeriodicExampleWorker struct {
	river.WorkerDefaults[durablePeriodicExampleArgs]
}

func (w *durablePeriodicExampleWorker) Work(context.Context, *river.Job[durablePeriodicExampleArgs]) error {
	return nil
}

type onceNowThenLaterSchedule struct{ calledOnce bool }

func (s *onceNowThenLaterSchedule) Next(t time.Time) time.Time {
	if s.calledOnce {
		return t.Add(15 * time.Minute)
	}
	s.calledOnce = true
	return t
}

func TestExample_DurablePeriodicJob_Compat(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	dbPool := riversharedtest.DBPool(ctx, t)
	driver := riverpgxv5.New(dbPool)
	schema := riverdbtest.TestSchema(ctx, t, driver, nil)

	workers := river.NewWorkers()
	river.AddWorker(workers, &durablePeriodicExampleWorker{})

	client, err := river.NewClient[pgx.Tx](driver, &river.Config{
		DurablePeriodicJobs: &river.DurablePeriodicJobsConfig{Enabled: true},
		PeriodicJobs: []*river.PeriodicJob{
			river.NewPeriodicJob(
				&onceNowThenLaterSchedule{},
				func() (river.JobArgs, *river.InsertOpts) { return durablePeriodicExampleArgs{}, nil },
				&river.PeriodicJobOpts{ID: "durable_periodic_example_id"},
			),
		},
		Queues: map[string]river.QueueConfig{river.QueueDefault: {MaxWorkers: 5}},
		Schema: schema, TestOnly: true, Workers: workers,
	})
	require.NoError(t, err)

	completed, cancel := client.Subscribe(river.EventKindJobCompleted)
	defer cancel()

	require.NoError(t, client.Start(ctx))
	riversharedtest.WaitOrTimeoutN(t, completed, 1)
	require.NoError(t, client.Stop(ctx))
}

type ephemeralExampleArgs struct{}

func (ephemeralExampleArgs) Kind() string                       { return "ephemeral_example" }
func (ephemeralExampleArgs) EphemeralOpts() river.EphemeralOpts { return river.EphemeralOpts{} }

type ephemeralExampleWorker struct {
	river.WorkerDefaults[ephemeralExampleArgs]
}

func (w *ephemeralExampleWorker) Work(context.Context, *river.Job[ephemeralExampleArgs]) error {
	return nil
}

func TestExample_EphemeralJob_Compat(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	dbPool := riversharedtest.DBPool(ctx, t)
	driver := riverpgxv5.New(dbPool)
	schema := riverdbtest.TestSchema(ctx, t, driver, nil)

	workers := river.NewWorkers()
	river.AddWorker(workers, &ephemeralExampleWorker{})

	client, err := river.NewClient[pgx.Tx](driver, &river.Config{
		Queues: map[string]river.QueueConfig{river.QueueDefault: {MaxWorkers: 10}},
		Schema: schema, TestOnly: true, Workers: workers,
	})
	require.NoError(t, err)

	completed, cancel := client.Subscribe(river.EventKindJobCompleted)
	defer cancel()

	inserted, err := client.Insert(ctx, ephemeralExampleArgs{}, nil)
	require.NoError(t, err)

	require.NoError(t, client.Start(ctx))
	riversharedtest.WaitOrTimeoutN(t, completed, 1)
	require.NoError(t, client.Stop(ctx))

	_, err = client.JobGet(ctx, inserted.Job.ID)
	require.ErrorIs(t, err, rivertype.ErrNotFound)
}

type noOpExampleArgs struct{}

func (noOpExampleArgs) Kind() string { return "noop_example" }

type noOpExampleWorker struct {
	river.WorkerDefaults[noOpExampleArgs]
}

func (w *noOpExampleWorker) Work(context.Context, *river.Job[noOpExampleArgs]) error { return nil }

func TestExample_EphemeralQueue_Compat(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	dbPool := riversharedtest.DBPool(ctx, t)
	driver := riverpgxv5.New(dbPool)
	schema := riverdbtest.TestSchema(ctx, t, driver, nil)

	workers := river.NewWorkers()
	river.AddWorker(workers, &noOpExampleWorker{})

	client, err := river.NewClient[pgx.Tx](driver, &river.Config{
		Queues: map[string]river.QueueConfig{
			"my_ephemeral_queue": {Ephemeral: &river.QueueEphemeralConfig{Enabled: true}, MaxWorkers: 10},
		},
		Schema: schema, TestOnly: true, Workers: workers,
	})
	require.NoError(t, err)

	completed, cancel := client.Subscribe(river.EventKindJobCompleted)
	defer cancel()

	inserted, err := client.Insert(ctx, noOpExampleArgs{}, &river.InsertOpts{Queue: "my_ephemeral_queue"})
	require.NoError(t, err)

	require.NoError(t, client.Start(ctx))
	riversharedtest.WaitOrTimeoutN(t, completed, 1)
	require.NoError(t, client.Stop(ctx))

	_, err = client.JobGet(ctx, inserted.Job.ID)
	require.ErrorIs(t, err, rivertype.ErrNotFound)
}

type concurrencyExampleArgs struct{}

func (concurrencyExampleArgs) Kind() string { return "concurrency_limited_example" }

type concurrencyExampleWorker struct {
	river.WorkerDefaults[concurrencyExampleArgs]

	running *atomic.Int64
	maxSeen *atomic.Int64
}

func (w *concurrencyExampleWorker) Work(ctx context.Context, job *river.Job[concurrencyExampleArgs]) error {
	nowRunning := w.running.Add(1)
	defer w.running.Add(-1)

	for {
		cur := w.maxSeen.Load()
		if nowRunning <= cur || w.maxSeen.CompareAndSwap(cur, nowRunning) {
			break
		}
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(80 * time.Millisecond):
		return nil
	}
}

func TestExample_GlobalConcurrencyLimiting_Compat(t *testing.T) {
	t.Parallel()

	ctx := t.Context()

	dbPool := riversharedtest.DBPool(ctx, t)
	driver := riverpgxv5.New(dbPool)
	schema := riverdbtest.TestSchema(ctx, t, driver, nil)

	workers := river.NewWorkers()
	stateRunning := &atomic.Int64{}
	stateMaxSeen := &atomic.Int64{}
	river.AddWorker(workers, &concurrencyExampleWorker{running: stateRunning, maxSeen: stateMaxSeen})

	newClient := func() (*river.Client[pgx.Tx], <-chan *river.Event, func()) {
		client, err := river.NewClient[pgx.Tx](driver, &river.Config{
			FetchCooldown:     25 * time.Millisecond,
			FetchPollInterval: 25 * time.Millisecond,
			Queues: map[string]river.QueueConfig{
				river.QueueDefault: {
					Concurrency: &river.ConcurrencyConfig{GlobalLimit: 2, LocalLimit: 1, Partition: river.PartitionConfig{ByKind: true}},
					MaxWorkers:  10,
				},
			},
			Schema: schema, TestOnly: true, Workers: workers,
		})
		require.NoError(t, err)
		require.NoError(t, client.Start(ctx))
		completed, stopSub := client.Subscribe(river.EventKindJobCompleted)
		return client, completed, stopSub
	}

	client1, completed1, stopSub1 := newClient()
	defer stopSub1()
	defer func() { _ = client1.Stop(context.Background()) }()

	client2, completed2, stopSub2 := newClient()
	defer stopSub2()
	defer func() { _ = client2.Stop(context.Background()) }()

	jobs := make([]river.InsertManyParams, 10)
	for i := range jobs {
		jobs[i] = river.InsertManyParams{Args: concurrencyExampleArgs{}}
	}
	_, err := client1.InsertMany(ctx, jobs)
	require.NoError(t, err)

	completed := 0
	deadline := time.After(20 * time.Second)
	for completed < 10 {
		select {
		case <-deadline:
			t.Fatalf("timed out waiting for jobs to complete; got %d", completed)
		case <-completed1:
			completed++
		case <-completed2:
			completed++
		}
	}

	require.LessOrEqual(t, stateMaxSeen.Load(), int64(2))
}

func TestExample_PerQueueRetention_Compat(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	dbPool := riversharedtest.DBPool(ctx, t)
	driver := riverpgxv5.New(dbPool)
	schema := riverdbtest.TestSchema(ctx, t, driver, nil)

	workers := river.NewWorkers()
	river.AddWorker(workers, &noOpExampleWorker{})

	_, err := river.NewClient[pgx.Tx](driver, &river.Config{
		Queues: map[string]river.QueueConfig{
			river.QueueDefault: {
				CancelledJobRetentionPeriod: 24 * time.Hour,
				CompletedJobRetentionPeriod: 24 * time.Hour,
				DiscardedJobRetentionPeriod: 7 * 24 * time.Hour,
				MaxWorkers:                  100,
			},
		},
		Schema: schema, TestOnly: true, Workers: workers,
	})
	require.NoError(t, err)
}

type workflowAArgs struct{}

func (workflowAArgs) Kind() string { return "workflow_loaddeps_a" }

type workflowBArgs struct{}

func (workflowBArgs) Kind() string { return "workflow_loaddeps_b" }

type workflowAWorker struct {
	river.WorkerDefaults[workflowAArgs]
}

func (w *workflowAWorker) Work(ctx context.Context, job *river.Job[workflowAArgs]) error {
	return river.RecordOutput(ctx, map[string]string{"result": "ok"})
}

type workflowBWorker struct {
	river.WorkerDefaults[workflowBArgs]
}

func (w *workflowBWorker) Work(context.Context, *river.Job[workflowBArgs]) error { return nil }

func TestExample_WorkflowLoadDepsAndLoadOutput_Compat(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	dbPool := riversharedtest.DBPool(ctx, t)
	driver := riverpgxv5.New(dbPool)
	schema := riverdbtest.TestSchema(ctx, t, driver, nil)

	workers := river.NewWorkers()
	river.AddWorker(workers, &workflowAWorker{})
	river.AddWorker(workers, &workflowBWorker{})

	client, err := river.NewClient[pgx.Tx](driver, &river.Config{
		Queues: map[string]river.QueueConfig{river.QueueDefault: {MaxWorkers: 10}},
		Schema: schema, TestOnly: true, Workers: workers,
	})
	require.NoError(t, err)

	require.NoError(t, client.Start(ctx))
	t.Cleanup(func() { _ = client.Stop(context.Background()) })

	workflow := client.NewWorkflow(&river.WorkflowOpts{ID: "wf_loaddeps_output_compat"})
	taskA := workflow.Add("task_a", workflowAArgs{}, nil, nil)
	workflow.Add("task_b", workflowBArgs{}, nil, &river.WorkflowTaskOpts{Deps: []string{taskA.Name}})
	prepared, err := workflow.Prepare(ctx)
	require.NoError(t, err)

	_, err = client.InsertMany(ctx, prepared.Jobs)
	require.NoError(t, err)

	completed, cancel := client.Subscribe(river.EventKindJobCompleted)
	defer cancel()
	riversharedtest.WaitOrTimeoutN(t, completed, 2)

	deps, err := workflow.LoadDeps(ctx, "task_b", nil)
	require.NoError(t, err)
	require.NotNil(t, deps.Get("task_b"))
	require.NotNil(t, deps.Get("task_a"))

	var output struct {
		Result string `json:"result"`
	}
	require.NoError(t, workflow.LoadOutput(ctx, "task_a", &output))
	require.Equal(t, "ok", output.Result)

	var noOutput struct{}
	err = workflow.LoadOutput(ctx, "task_b", &noOutput)
	require.Error(t, err)
	var noOutputErr *river.TaskHasNoOutputError
	require.ErrorAs(t, err, &noOutputErr)
}

type workflowMultiChildArgs struct {
	RunID string `json:"run_id"`
	Index int    `json:"index"`
}

func (workflowMultiChildArgs) Kind() string { return "workflow_multi_child" }

type workflowMultiFinalizeArgs struct {
	RunID string `json:"run_id"`
}

func (workflowMultiFinalizeArgs) Kind() string { return "workflow_multi_finalize" }

type workflowMultiChildWorker struct {
	river.WorkerDefaults[workflowMultiChildArgs]

	running *atomic.Int64
	maxSeen *atomic.Int64
}

func (w *workflowMultiChildWorker) Work(ctx context.Context, job *river.Job[workflowMultiChildArgs]) error {
	nowRunning := w.running.Add(1)
	defer w.running.Add(-1)

	for {
		cur := w.maxSeen.Load()
		if nowRunning <= cur || w.maxSeen.CompareAndSwap(cur, nowRunning) {
			break
		}
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(80 * time.Millisecond):
	}

	return river.RecordOutput(ctx, map[string]int{"value": job.Args.Index + 1})
}

type workflowMultiFinalizeWorker struct {
	river.WorkerDefaults[workflowMultiFinalizeArgs]

	client *river.Client[pgx.Tx]
}

func (w *workflowMultiFinalizeWorker) Work(ctx context.Context, job *river.Job[workflowMultiFinalizeArgs]) error {
	workflow := w.client.NewWorkflow(&river.WorkflowOpts{ID: job.Args.RunID})
	deps, err := workflow.LoadDeps(ctx, "parent", nil)
	if err != nil {
		return err
	}

	sum := 0
	count := 0
	for taskName, task := range deps.ByName {
		if taskName == "parent" {
			continue
		}
		var out struct {
			Value int `json:"value"`
		}
		if err := task.Output(&out); err != nil {
			return err
		}
		sum += out.Value
		count++
	}

	return river.RecordOutput(ctx, map[string]int{
		"sum":   sum,
		"count": count,
	})
}

func TestExample_WorkflowParentAfterAllChildren_MultiWorker_Compat(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	dbPool := riversharedtest.DBPool(ctx, t)
	driver := riverpgxv5.New(dbPool)
	schema := riverdbtest.TestSchema(ctx, t, driver, nil)

	workers := river.NewWorkers()
	running := &atomic.Int64{}
	maxSeen := &atomic.Int64{}
	river.AddWorker(workers, &workflowMultiChildWorker{running: running, maxSeen: maxSeen})

	client, err := river.NewClient[pgx.Tx](driver, &river.Config{
		Queues: map[string]river.QueueConfig{river.QueueDefault: {MaxWorkers: 4}},
		Schema: schema, TestOnly: true, Workers: workers,
	})
	require.NoError(t, err)

	river.AddWorker(workers, &workflowMultiFinalizeWorker{client: client})

	require.NoError(t, client.Start(ctx))
	t.Cleanup(func() { _ = client.Stop(context.Background()) })

	runID := "wf_multi_worker_parent"
	workflow := client.NewWorkflow(&river.WorkflowOpts{ID: runID})

	depNames := make([]string, 0, 6)
	for i := range 6 {
		name := fmt.Sprintf("child_%d", i)
		task := workflow.Add(name, workflowMultiChildArgs{RunID: runID, Index: i}, nil, nil)
		depNames = append(depNames, task.Name)
	}
	workflow.Add("parent", workflowMultiFinalizeArgs{RunID: runID}, nil, &river.WorkflowTaskOpts{Deps: depNames})

	prepared, err := workflow.Prepare(ctx)
	require.NoError(t, err)

	_, err = client.InsertMany(ctx, prepared.Jobs)
	require.NoError(t, err)

	completed, cancel := client.Subscribe(river.EventKindJobCompleted)
	defer cancel()
	riversharedtest.WaitOrTimeoutN(t, completed, 7)

	require.GreaterOrEqual(t, maxSeen.Load(), int64(2), "expected child jobs to run concurrently on multiple workers")

	var parentOut struct {
		Sum   int `json:"sum"`
		Count int `json:"count"`
	}
	require.NoError(t, workflow.LoadOutput(ctx, "parent", &parentOut))
	require.Equal(t, 6, parentOut.Count)
	require.Equal(t, 21, parentOut.Sum)
}

type workflowDistributedChildWorker struct {
	river.WorkerDefaults[workflowMultiChildArgs]

	node string
}

func (w *workflowDistributedChildWorker) Work(ctx context.Context, job *river.Job[workflowMultiChildArgs]) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(120 * time.Millisecond):
	}

	return river.RecordOutput(ctx, map[string]any{
		"value": job.Args.Index + 1,
		"node":  w.node,
	})
}

func TestExample_WorkflowParentAfterAllChildren_TwoClients_Compat(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	dbPool := riversharedtest.DBPool(ctx, t)
	driver := riverpgxv5.New(dbPool)
	schema := riverdbtest.TestSchema(ctx, t, driver, nil)

	newClient := func(nodeID string) *river.Client[pgx.Tx] {
		workers := river.NewWorkers()
		childWorker := &workflowDistributedChildWorker{node: nodeID}
		river.AddWorker(workers, childWorker)

		client, err := river.NewClient[pgx.Tx](driver, &river.Config{
			ID: nodeID,
			Queues: map[string]river.QueueConfig{
				river.QueueDefault: {
					FetchCooldown:     10 * time.Millisecond,
					FetchPollInterval: 10 * time.Millisecond,
					MaxWorkers:        2,
				},
			},
			Schema: schema, TestOnly: true, Workers: workers,
		})
		require.NoError(t, err)

		river.AddWorker(workers, &workflowMultiFinalizeWorker{client: client})
		return client
	}

	clientA := newClient("node_a")
	clientB := newClient("node_b")

	require.NoError(t, clientA.Start(ctx))
	t.Cleanup(func() { _ = clientA.Stop(context.Background()) })
	require.NoError(t, clientB.Start(ctx))
	t.Cleanup(func() { _ = clientB.Stop(context.Background()) })

	runID := "wf_two_clients_parent"
	workflow := clientA.NewWorkflow(&river.WorkflowOpts{ID: runID})

	depNames := make([]string, 0, 10)
	for i := range 10 {
		name := fmt.Sprintf("child_%d", i)
		task := workflow.Add(name, workflowMultiChildArgs{RunID: runID, Index: i}, nil, nil)
		depNames = append(depNames, task.Name)
	}
	workflow.Add("parent", workflowMultiFinalizeArgs{RunID: runID}, nil, &river.WorkflowTaskOpts{Deps: depNames})

	prepared, err := workflow.Prepare(ctx)
	require.NoError(t, err)

	_, err = clientA.InsertMany(ctx, prepared.Jobs)
	require.NoError(t, err)

	completedA, cancelA := clientA.Subscribe(river.EventKindJobCompleted)
	defer cancelA()
	completedB, cancelB := clientB.Subscribe(river.EventKindJobCompleted)
	defer cancelB()

	completed := 0
	deadline := time.After(30 * time.Second)
	for completed < 11 {
		select {
		case <-deadline:
			t.Fatalf("timed out waiting for workflow completion; got %d events", completed)
		case <-completedA:
			completed++
		case <-completedB:
			completed++
		}
	}

	deps, err := workflow.LoadDeps(ctx, "parent", nil)
	require.NoError(t, err)

	nodesSeen := map[string]struct{}{}
	for taskName, task := range deps.ByName {
		if taskName == "parent" {
			continue
		}
		var out struct {
			Value int    `json:"value"`
			Node  string `json:"node"`
		}
		require.NoError(t, task.Output(&out))
		require.NotEmpty(t, out.Node)
		nodesSeen[out.Node] = struct{}{}
	}

	require.Contains(t, nodesSeen, "node_a")
	require.Contains(t, nodesSeen, "node_b")

	var parentOut struct {
		Sum   int `json:"sum"`
		Count int `json:"count"`
	}
	require.NoError(t, workflow.LoadOutput(ctx, "parent", &parentOut))
	require.Equal(t, 10, parentOut.Count)
	require.Equal(t, 55, parentOut.Sum)
}
