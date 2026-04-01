package river

import (
	"context"
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
)

type concurrencyCompatArgs struct {
	CustomerID int    `json:"customer_id"`
	TraceID    string `json:"trace_id"`
}

func (concurrencyCompatArgs) Kind() string { return "concurrency_compat" }

type concurrencyCompatWorker struct {
	WorkerDefaults[concurrencyCompatArgs]

	running       *atomic.Int64
	maxSeen       *atomic.Int64
	mu            *sync.Mutex
	runningByKey  map[string]int
	maxSeenByKey  map[string]int
	partitionFunc func(concurrencyCompatArgs) string
}

func (w *concurrencyCompatWorker) Work(ctx context.Context, job *Job[concurrencyCompatArgs]) error {
	key := w.partitionFunc(job.Args)
	nowRunning := w.running.Add(1)
	defer w.running.Add(-1)

	for {
		cur := w.maxSeen.Load()
		if nowRunning <= cur || w.maxSeen.CompareAndSwap(cur, nowRunning) {
			break
		}
	}

	w.mu.Lock()
	w.runningByKey[key]++
	if w.runningByKey[key] > w.maxSeenByKey[key] {
		w.maxSeenByKey[key] = w.runningByKey[key]
	}
	w.mu.Unlock()

	defer func() {
		w.mu.Lock()
		w.runningByKey[key]--
		w.mu.Unlock()
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(80 * time.Millisecond):
		return nil
	}
}

func testConcurrencyClient(t *testing.T, ctx context.Context, driver *riverpgxv5.Driver, schema string, worker *concurrencyCompatWorker, queueConfig QueueConfig) (*Client[pgx.Tx], <-chan *Event, func()) {
	t.Helper()

	workers := NewWorkers()
	AddWorker(workers, worker)

	client, err := NewClient[pgx.Tx](driver, &Config{
		FetchCooldown:     25 * time.Millisecond,
		FetchPollInterval: 25 * time.Millisecond,
		Queues: map[string]QueueConfig{
			QueueDefault: queueConfig,
		},
		Schema:   schema,
		TestOnly: true,
		Workers:  workers,
	})
	require.NoError(t, err)
	require.NoError(t, client.Start(ctx))
	completed, stopSub := client.Subscribe(EventKindJobCompleted)
	return client, completed, stopSub
}

func waitForCompletions(t *testing.T, n int, chans ...<-chan *Event) {
	t.Helper()
	completed := 0
	deadline := time.After(20 * time.Second)
	for completed < n {
		select {
		case <-deadline:
			t.Fatalf("timed out waiting for jobs to complete; got %d", completed)
		case <-chans[0]:
			completed++
		case <-func() <-chan *Event {
			if len(chans) > 1 {
				return chans[1]
			}
			return nil
		}():
			completed++
		}
	}
}

func TestConcurrency_GlobalLimit_Compat(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dbPool := riversharedtest.DBPool(ctx, t)
	driver := riverpgxv5.New(dbPool)
	schema := riverdbtest.TestSchema(ctx, t, driver, nil)

	worker := &concurrencyCompatWorker{
		running:       &atomic.Int64{},
		maxSeen:       &atomic.Int64{},
		mu:            &sync.Mutex{},
		runningByKey:  map[string]int{},
		maxSeenByKey:  map[string]int{},
		partitionFunc: func(_ concurrencyCompatArgs) string { return "all" },
	}

	queueConfig := QueueConfig{Concurrency: &ConcurrencyConfig{GlobalLimit: 2}, MaxWorkers: 10}
	client1, completed1, stopSub1 := testConcurrencyClient(t, ctx, driver, schema, worker, queueConfig)
	defer stopSub1()
	defer func() { _ = client1.Stop(context.Background()) }()

	jobs := make([]InsertManyParams, 10)
	for i := range jobs {
		jobs[i] = InsertManyParams{Args: concurrencyCompatArgs{CustomerID: i % 2, TraceID: fmt.Sprintf("trace-%d", i)}}
	}
	_, err := client1.InsertMany(ctx, jobs)
	require.NoError(t, err)

	waitForCompletions(t, 10, completed1)
	require.LessOrEqual(t, worker.maxSeen.Load(), int64(2))
}

func TestConcurrency_LocalLimit_Compat(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	dbPool := riversharedtest.DBPool(ctx, t)
	driver := riverpgxv5.New(dbPool)
	schema := riverdbtest.TestSchema(ctx, t, driver, nil)

	worker := &concurrencyCompatWorker{
		running:       &atomic.Int64{},
		maxSeen:       &atomic.Int64{},
		mu:            &sync.Mutex{},
		runningByKey:  map[string]int{},
		maxSeenByKey:  map[string]int{},
		partitionFunc: func(_ concurrencyCompatArgs) string { return "all" },
	}

	client, completed, stopSub := testConcurrencyClient(t, ctx, driver, schema, worker, QueueConfig{Concurrency: &ConcurrencyConfig{LocalLimit: 1}, MaxWorkers: 10})
	defer stopSub()
	defer func() { _ = client.Stop(context.Background()) }()

	jobs := make([]InsertManyParams, 8)
	for i := range jobs {
		jobs[i] = InsertManyParams{Args: concurrencyCompatArgs{CustomerID: i % 2, TraceID: fmt.Sprintf("trace-%d", i)}}
	}
	_, err := client.InsertMany(ctx, jobs)
	require.NoError(t, err)

	waitForCompletions(t, 8, completed)
	require.LessOrEqual(t, worker.maxSeen.Load(), int64(1))
}

func TestConcurrency_ByArgsPartition_Compat(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dbPool := riversharedtest.DBPool(ctx, t)
	driver := riverpgxv5.New(dbPool)
	schema := riverdbtest.TestSchema(ctx, t, driver, nil)

	worker := &concurrencyCompatWorker{
		running:      &atomic.Int64{},
		maxSeen:      &atomic.Int64{},
		mu:           &sync.Mutex{},
		runningByKey: map[string]int{},
		maxSeenByKey: map[string]int{},
		partitionFunc: func(args concurrencyCompatArgs) string {
			return fmt.Sprintf("customer:%d", args.CustomerID)
		},
	}

	queueConfig := QueueConfig{Concurrency: &ConcurrencyConfig{GlobalLimit: 1, LocalLimit: 1, Partition: PartitionConfig{ByArgs: []string{"customer_id"}}}, MaxWorkers: 10}
	client1, completed1, stopSub1 := testConcurrencyClient(t, ctx, driver, schema, worker, queueConfig)
	defer stopSub1()
	defer func() { _ = client1.Stop(context.Background()) }()

	jobs := make([]InsertManyParams, 10)
	for i := range jobs {
		jobs[i] = InsertManyParams{Args: concurrencyCompatArgs{CustomerID: (i % 2) + 1, TraceID: fmt.Sprintf("trace-%d", i)}}
	}
	_, err := client1.InsertMany(ctx, jobs)
	require.NoError(t, err)

	waitForCompletions(t, 10, completed1)
	require.LessOrEqual(t, worker.maxSeenByKey["customer:1"], 1)
	require.LessOrEqual(t, worker.maxSeenByKey["customer:2"], 1)
}

func TestConcurrency_ByArgsEmptySliceUsesAllArgs_Compat(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dbPool := riversharedtest.DBPool(ctx, t)
	driver := riverpgxv5.New(dbPool)
	schema := riverdbtest.TestSchema(ctx, t, driver, nil)

	worker := &concurrencyCompatWorker{
		running:      &atomic.Int64{},
		maxSeen:      &atomic.Int64{},
		mu:           &sync.Mutex{},
		runningByKey: map[string]int{},
		maxSeenByKey: map[string]int{},
		partitionFunc: func(args concurrencyCompatArgs) string {
			return fmt.Sprintf("customer:%d|trace:%s", args.CustomerID, args.TraceID)
		},
	}

	queueConfig := QueueConfig{Concurrency: &ConcurrencyConfig{GlobalLimit: 1, LocalLimit: 1, Partition: PartitionConfig{ByArgs: []string{}}}, MaxWorkers: 10}
	client1, completed1, stopSub1 := testConcurrencyClient(t, ctx, driver, schema, worker, queueConfig)
	defer stopSub1()
	defer func() { _ = client1.Stop(context.Background()) }()
	jobs := []InsertManyParams{
		{Args: concurrencyCompatArgs{CustomerID: 1, TraceID: "a"}},
		{Args: concurrencyCompatArgs{CustomerID: 1, TraceID: "a"}},
		{Args: concurrencyCompatArgs{CustomerID: 1, TraceID: "b"}},
		{Args: concurrencyCompatArgs{CustomerID: 1, TraceID: "b"}},
	}
	_, err := client1.InsertMany(ctx, jobs)
	require.NoError(t, err)

	waitForCompletions(t, 4, completed1)
	require.LessOrEqual(t, worker.maxSeenByKey["customer:1|trace:a"], 1)
	require.LessOrEqual(t, worker.maxSeenByKey["customer:1|trace:b"], 1)
}
