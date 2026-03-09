package river_test

import (
	"context"
	"errors"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverbatch"
	"github.com/riverqueue/river/riverdbtest"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivershared/riversharedtest"
)

type batchExampleArgs struct {
	InstanceID int64 `json:"instance_id" river:"batch"`
}

func (batchExampleArgs) Kind() string { return "batch_example" }

func (batchExampleArgs) BatchOpts() river.BatchOpts { return river.BatchOpts{} }

type batchExampleWorker struct {
	river.WorkerDefaults[batchExampleArgs]

	mu      sync.Mutex
	batches [][]int64
}

func (w *batchExampleWorker) Work(ctx context.Context, job *river.Job[batchExampleArgs]) error {
	return riverbatch.Work[batchExampleArgs](ctx, w, job, &riverbatch.WorkerOpts{
		MaxCount:     5,
		MaxDelay:     10 * time.Millisecond,
		PollInterval: 10 * time.Millisecond,
	})
}

func (w *batchExampleWorker) NextRetry(_ *river.Job[batchExampleArgs]) time.Time { return time.Now() }

func (w *batchExampleWorker) WorkMany(_ context.Context, jobs []*river.Job[batchExampleArgs]) error {
	ids := make([]int64, 0, len(jobs))
	for _, job := range jobs {
		ids = append(ids, job.Args.InstanceID)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })

	w.mu.Lock()
	w.batches = append(w.batches, ids)
	w.mu.Unlock()

	multiErr := riverbatch.NewMultiError()
	for _, job := range jobs {
		if job.Args.InstanceID%10 == 0 && job.Attempt == 1 {
			multiErr.AddByID(job.ID, errors.New("transient failure"))
		}
	}
	return multiErr
}

func TestExample_BatchWorker_Compat(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	dbPool := riversharedtest.DBPool(ctx, t)
	driver := riverpgxv5.New(dbPool)
	schema := riverdbtest.TestSchema(ctx, t, driver, nil)

	workers := river.NewWorkers()
	worker := &batchExampleWorker{}
	river.AddWorker(workers, worker)

	client, err := river.NewClient[pgx.Tx](driver, &river.Config{
		FetchCooldown:     50 * time.Millisecond,
		FetchPollInterval: 50 * time.Millisecond,
		Queues: map[string]river.QueueConfig{
			river.QueueDefault: {
				MaxWorkers: 1,
			},
		},
		Schema:   schema,
		TestOnly: true,
		Workers:  workers,
	})
	require.NoError(t, err)

	require.NoError(t, client.Start(ctx))
	t.Cleanup(func() { _ = client.Stop(ctx) })

	subscribeChan, cancel := client.Subscribe(river.EventKindJobCompleted)
	defer cancel()

	insertParams := make([]river.InsertManyParams, 10)
	for i := range insertParams {
		insertParams[i] = river.InsertManyParams{Args: batchExampleArgs{InstanceID: int64(i) + 1}}
	}

	_, err = client.InsertMany(ctx, insertParams)
	require.NoError(t, err)

	riversharedtest.WaitOrTimeoutN(t, subscribeChan, 10)
	require.NoError(t, client.Stop(ctx))

	worker.mu.Lock()
	batches := append([][]int64(nil), worker.batches...)
	worker.mu.Unlock()

	require.NotEmpty(t, batches)

	seenTen := 0
	seenMultiBatch := false
	for _, batch := range batches {
		if len(batch) > 1 {
			seenMultiBatch = true
		}
		for _, id := range batch {
			if id == 10 {
				seenTen++
			}
		}
	}

	require.True(t, seenMultiBatch, "expected at least one multi-job batch")
	require.GreaterOrEqual(t, seenTen, 2, "expected ID 10 to run at least twice (retry)")
}
