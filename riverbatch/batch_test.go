package riverbatch

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river"
	"github.com/riverqueue/river/rivertype"
)

type batchTestArgs struct{}

func (batchTestArgs) Kind() string               { return "batch_test_args" }
func (batchTestArgs) BatchOpts() river.BatchOpts { return river.BatchOpts{} }

type batchWorker struct {
	mu         sync.Mutex
	calls      int
	batchSizes []int
	fn         func(context.Context, []*river.Job[batchTestArgs]) error
}

func (w *batchWorker) WorkMany(ctx context.Context, jobs []*river.Job[batchTestArgs]) error {
	w.mu.Lock()
	w.calls++
	w.batchSizes = append(w.batchSizes, len(jobs))
	w.mu.Unlock()

	if w.fn != nil {
		return w.fn(ctx, jobs)
	}
	return nil
}

func mkJob(id int64) *river.Job[batchTestArgs] {
	return &river.Job[batchTestArgs]{
		JobRow: &rivertype.JobRow{ID: id, Kind: batchTestArgs{}.Kind()},
		Args:   batchTestArgs{},
	}
}

func TestWork_FlushesOnMaxCount(t *testing.T) {
	resetBatchState()

	ctx := context.Background()
	worker := &batchWorker{}
	opts := &WorkerOpts{MaxCount: 2, MaxDelay: time.Second}

	errCh := make(chan error, 2)
	go func() { errCh <- Work(ctx, worker, mkJob(1), opts) }()
	go func() { errCh <- Work(ctx, worker, mkJob(2), opts) }()

	require.NoError(t, <-errCh)
	require.NoError(t, <-errCh)

	worker.mu.Lock()
	defer worker.mu.Unlock()
	require.Equal(t, 1, worker.calls)
	require.Equal(t, []int{2}, worker.batchSizes)
}

func TestWork_FlushesOnMaxDelay(t *testing.T) {
	resetBatchState()

	ctx := context.Background()
	worker := &batchWorker{}
	opts := &WorkerOpts{MaxCount: 10, MaxDelay: 50 * time.Millisecond}

	start := time.Now()
	require.NoError(t, Work(ctx, worker, mkJob(1), opts))
	require.GreaterOrEqual(t, time.Since(start), 45*time.Millisecond)

	worker.mu.Lock()
	defer worker.mu.Unlock()
	require.Equal(t, 1, worker.calls)
	require.Equal(t, []int{1}, worker.batchSizes)
}

func TestWork_MultiErrorFanout(t *testing.T) {
	resetBatchState()

	ctx := context.Background()
	targetErr := errors.New("job failed")
	worker := &batchWorker{fn: func(_ context.Context, jobs []*river.Job[batchTestArgs]) error {
		m := NewMultiError()
		for _, job := range jobs {
			if job.ID == 2 {
				m.AddByID(job.ID, targetErr)
			}
		}
		return m
	}}

	opts := &WorkerOpts{MaxCount: 2, MaxDelay: time.Second}

	errCh := make(chan error, 2)
	go func() { errCh <- Work(ctx, worker, mkJob(1), opts) }()
	go func() { errCh <- Work(ctx, worker, mkJob(2), opts) }()

	err1 := <-errCh
	err2 := <-errCh
	require.True(t, err1 == nil || errors.Is(err1, targetErr))
	require.True(t, err2 == nil || errors.Is(err2, targetErr))
	require.True(t, errors.Is(err1, targetErr) || errors.Is(err2, targetErr))
}

func resetBatchState() {
	batchMu.Lock()
	defer batchMu.Unlock()
	batchGroups = map[string]any{}
}
