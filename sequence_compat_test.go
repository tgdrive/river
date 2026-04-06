package river

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/riverdbtest"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivershared/riversharedtest"
	"github.com/riverqueue/river/rivertype"
)

type sequenceCompatArgs struct {
	CustomerID          string `json:"customer_id"                     river:"sequence"`
	Behavior            string `json:"behavior"`
	ContinueOnCancelled bool   `json:"continue_on_cancelled,omitempty"`
	ContinueOnDiscarded bool   `json:"continue_on_discarded,omitempty"`
}

func (a sequenceCompatArgs) Kind() string { return "sequence_compat" }
func (a sequenceCompatArgs) SequenceOpts() SequenceOpts {
	return SequenceOpts{ByArgs: true, ContinueOnCancelled: a.ContinueOnCancelled, ContinueOnDiscarded: a.ContinueOnDiscarded}
}

type sequenceCompatState struct {
	mu                sync.Mutex
	runningByCustomer map[string]int
	maxByCustomer     map[string]int
	cancelledOnce     map[string]bool
}

type sequenceCompatWorker struct {
	WorkerDefaults[sequenceCompatArgs]

	state *sequenceCompatState
}

func (w *sequenceCompatWorker) Work(ctx context.Context, job *Job[sequenceCompatArgs]) error {
	args := job.Args
	w.state.mu.Lock()
	w.state.runningByCustomer[args.CustomerID]++
	if w.state.runningByCustomer[args.CustomerID] > w.state.maxByCustomer[args.CustomerID] {
		w.state.maxByCustomer[args.CustomerID] = w.state.runningByCustomer[args.CustomerID]
	}
	w.state.mu.Unlock()
	defer func() {
		w.state.mu.Lock()
		w.state.runningByCustomer[args.CustomerID]--
		w.state.mu.Unlock()
	}()

	time.Sleep(50 * time.Millisecond)

	switch args.Behavior {
	case "cancel":
		return JobCancel(errors.New("cancel"))
	case "cancel_once":
		w.state.mu.Lock()
		already := w.state.cancelledOnce[args.CustomerID]
		if !already {
			w.state.cancelledOnce[args.CustomerID] = true
		}
		w.state.mu.Unlock()
		if !already {
			return JobCancel(errors.New("cancel once"))
		}
	case "discard":
		return errors.New("discard")
	}
	return nil
}

func setupSequenceCompatClient(ctx context.Context, t *testing.T, queueConfig QueueConfig) (*Client[pgx.Tx], *sequenceCompatState) {
	t.Helper()
	dbPool := riversharedtest.DBPool(ctx, t)
	driver := riverpgxv5.New(dbPool)
	schema := riverdbtest.TestSchema(ctx, t, driver, nil)
	workers := NewWorkers()
	state := &sequenceCompatState{runningByCustomer: map[string]int{}, maxByCustomer: map[string]int{}, cancelledOnce: map[string]bool{}}
	AddWorker(workers, &sequenceCompatWorker{state: state})
	client, err := NewClient[pgx.Tx](driver, &Config{FetchCooldown: 10 * time.Millisecond, FetchPollInterval: 10 * time.Millisecond, Queues: map[string]QueueConfig{QueueDefault: queueConfig}, Schema: schema, SequenceSchedulerInterval: 10 * time.Millisecond, TestOnly: true, Workers: workers})
	require.NoError(t, err)
	return client, state
}

func waitForEvents(t *testing.T, ch <-chan *Event, n int) []*Event {
	t.Helper()
	out := make([]*Event, 0, n)
	deadline := time.After(15 * time.Second)
	for len(out) < n {
		select {
		case <-deadline:
			t.Fatalf("timed out waiting for %d events, got %d", n, len(out))
		case ev := <-ch:
			out = append(out, ev)
		}
	}
	return out
}

func TestSequence_SerialBySequence_Compat(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	client, state := setupSequenceCompatClient(ctx, t, QueueConfig{MaxWorkers: 10})
	completed, cancel := client.Subscribe(EventKindJobCompleted)
	defer cancel()
	require.NoError(t, client.Start(ctx))
	defer client.Stop(context.Background())
	jobs := []InsertManyParams{}
	for i := range 3 {
		jobs = append(jobs, InsertManyParams{Args: sequenceCompatArgs{CustomerID: "a", Behavior: fmt.Sprintf("ok-%d", i)}})
		jobs = append(jobs, InsertManyParams{Args: sequenceCompatArgs{CustomerID: "b", Behavior: fmt.Sprintf("ok-%d", i)}})
	}
	_, err := client.InsertMany(ctx, jobs)
	require.NoError(t, err)
	_ = waitForEvents(t, completed, 6)
	state.mu.Lock()
	defer state.mu.Unlock()
	require.LessOrEqual(t, state.maxByCustomer["a"], 1)
	require.LessOrEqual(t, state.maxByCustomer["b"], 1)
}

func TestSequence_HaltsOnCancelled_Compat(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	client, _ := setupSequenceCompatClient(ctx, t, QueueConfig{MaxWorkers: 10})
	sub, cancel := client.Subscribe(EventKindJobCancelled, EventKindJobCompleted)
	defer cancel()
	require.NoError(t, client.Start(ctx))
	defer client.Stop(context.Background())
	res, err := client.InsertMany(ctx, []InsertManyParams{{Args: sequenceCompatArgs{CustomerID: "halt", Behavior: "cancel"}}, {Args: sequenceCompatArgs{CustomerID: "halt", Behavior: "ok"}}})
	require.NoError(t, err)
	_ = waitForEvents(t, sub, 1)
	require.Eventually(t, func() bool {
		job, err := client.JobGet(ctx, res[1].Job.ID)
		return err == nil && job.State == rivertype.JobStatePending
	}, 5*time.Second, 25*time.Millisecond)
}

func TestSequence_ContinueOnCancelled_Compat(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	client, _ := setupSequenceCompatClient(ctx, t, QueueConfig{MaxWorkers: 10})
	sub, cancel := client.Subscribe(EventKindJobCancelled, EventKindJobCompleted)
	defer cancel()
	require.NoError(t, client.Start(ctx))
	defer client.Stop(context.Background())
	_, err := client.InsertMany(ctx, []InsertManyParams{{Args: sequenceCompatArgs{CustomerID: "continue", Behavior: "cancel", ContinueOnCancelled: true}}, {Args: sequenceCompatArgs{CustomerID: "continue", Behavior: "ok", ContinueOnCancelled: true}}})
	require.NoError(t, err)
	events := waitForEvents(t, sub, 2)
	require.Equal(t, EventKindJobCancelled, events[0].Kind)
	require.Equal(t, EventKindJobCompleted, events[1].Kind)
}

func TestSequence_RetryRecoverHaltedSequence_Compat(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	client, _ := setupSequenceCompatClient(ctx, t, QueueConfig{MaxWorkers: 10})
	sub, cancel := client.Subscribe(EventKindJobCancelled, EventKindJobCompleted)
	defer cancel()
	require.NoError(t, client.Start(ctx))
	defer client.Stop(context.Background())
	res, err := client.InsertMany(ctx, []InsertManyParams{{Args: sequenceCompatArgs{CustomerID: "recover", Behavior: "cancel_once"}}, {Args: sequenceCompatArgs{CustomerID: "recover", Behavior: "ok"}}})
	require.NoError(t, err)
	first := waitForEvents(t, sub, 1)
	require.Equal(t, EventKindJobCancelled, first[0].Kind)
	_, err = client.JobRetry(ctx, res[0].Job.ID)
	require.NoError(t, err)
	events := waitForEvents(t, sub, 2)
	require.Equal(t, EventKindJobCompleted, events[0].Kind)
	require.Equal(t, EventKindJobCompleted, events[1].Kind)
}
