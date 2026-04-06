package riverbatch

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"math"
	"sync"
	"time"

	"github.com/riverqueue/river"
	"github.com/riverqueue/river/rivertype"
)

const (
	MaxCountDefault     = 100
	MaxDelayDefault     = 5 * time.Second
	PollIntervalDefault = 1 * time.Second
)

type ManyWorker[T river.JobArgsWithBatch] interface {
	WorkMany(ctx context.Context, jobs []*river.Job[T]) error
}

type WorkerOpts struct {
	MaxCount     int
	MaxDelay     time.Duration
	PollInterval time.Duration
}

type batchItem[T river.JobArgsWithBatch] struct {
	job   *river.Job[T]
	errCh chan error
}

type batchGroup[T river.JobArgsWithBatch] struct {
	worker ManyWorker[T]
	opts   WorkerOpts
	items  []batchItem[T]
}

var (
	batchMu     sync.Mutex
	batchGroups = map[string]any{}
)

func (o *WorkerOpts) withDefaults() WorkerOpts {
	if o == nil {
		return WorkerOpts{MaxCount: MaxCountDefault, MaxDelay: MaxDelayDefault, PollInterval: PollIntervalDefault}
	}
	out := *o
	if out.MaxCount <= 0 {
		out.MaxCount = MaxCountDefault
	}
	if out.MaxDelay <= 0 {
		out.MaxDelay = MaxDelayDefault
	}
	if out.PollInterval <= 0 {
		out.PollInterval = PollIntervalDefault
	}
	return out
}

func validateWorkerOpts(opts *WorkerOpts) error {
	if opts == nil {
		return nil
	}
	if opts.MaxCount <= 0 || opts.MaxCount > math.MaxInt32 {
		return errors.New("MaxCount must be greater than 0 and less than math.MaxInt32")
	}
	if opts.MaxDelay < 0 {
		return errors.New("MaxDelay must be greater than or equal to 0")
	}
	if opts.PollInterval < 0 {
		return errors.New("PollInterval must be greater than or equal to 0")
	}
	return nil
}

func keyForJob[T river.JobArgsWithBatch](job *river.Job[T]) string {
	if job == nil || job.JobRow == nil || job.Kind == "" {
		return "batch"
	}

	batchKey := ""
	if len(job.Metadata) > 0 && !bytes.Equal(job.Metadata, []byte("{}")) {
		metadata := map[string]json.RawMessage{}
		if json.Unmarshal(job.Metadata, &metadata) == nil {
			if raw, ok := metadata["batch_key"]; ok {
				_ = json.Unmarshal(raw, &batchKey)
			}
		}
	}

	if batchKey == "" {
		batchKey = job.Kind
	}
	return job.Kind + ":" + batchKey
}

func flush[T river.JobArgsWithBatch](ctx context.Context, key string) {
	batchMu.Lock()
	raw, ok := batchGroups[key]
	if !ok {
		batchMu.Unlock()
		return
	}
	group, ok := raw.(*batchGroup[T])
	if !ok || len(group.items) == 0 {
		batchMu.Unlock()
		return
	}
	items := append([]batchItem[T]{}, group.items...)
	delete(batchGroups, key)
	batchMu.Unlock()

	jobs := make([]*river.Job[T], 0, len(items))
	for _, item := range items {
		jobs = append(jobs, item.job)
	}

	err := group.worker.WorkMany(ctx, jobs)
	multiErr := &MultiError{}
	hasMultiError := errors.As(err, &multiErr)

	for _, item := range items {
		itemErr := err
		if hasMultiError {
			itemErr = multiErr.GetByID(item.job.ID)
		}
		item.errCh <- itemErr
	}
}

func Work[T river.JobArgsWithBatch](ctx context.Context, worker ManyWorker[T], job *river.Job[T], opts *WorkerOpts) error {
	if worker == nil {
		return errors.New("worker must not be nil")
	}
	if job == nil {
		return errors.New("job must not be nil")
	}
	if err := validateWorkerOpts(opts); err != nil {
		return err
	}

	resolvedOpts := opts.withDefaults()
	batchClient, err := river.BatchClientFromContextSafely(ctx)
	if err != nil {
		return workInProcess(ctx, worker, job, resolvedOpts)
	}

	jobs := []*river.Job[T]{job}
	seenIDs := map[int64]struct{}{job.ID: {}}
	deadline := time.Now().Add(resolvedOpts.MaxDelay)
	batchKey := keyForJob(job)

	for len(jobs) < resolvedOpts.MaxCount {
		remaining := resolvedOpts.MaxCount - len(jobs)
		fetchedRows, err := batchClient.FetchBatch(ctx, job.JobRow, remaining)
		if err != nil {
			return err
		}

		for _, row := range fetchedRows {
			if row == nil {
				continue
			}
			if keyForFetched(row) != batchKey {
				continue
			}
			if _, exists := seenIDs[row.ID]; exists {
				continue
			}

			args := *new(T)
			if len(row.EncodedArgs) > 0 {
				if err := json.Unmarshal(row.EncodedArgs, &args); err != nil {
					return err
				}
			}

			jobs = append(jobs, &river.Job[T]{JobRow: row, Args: args})
			seenIDs[row.ID] = struct{}{}
			if len(jobs) >= resolvedOpts.MaxCount {
				break
			}
		}

		if len(jobs) >= resolvedOpts.MaxCount || time.Now().After(deadline) {
			break
		}

		sleepFor := resolvedOpts.PollInterval
		remainingTime := time.Until(deadline)
		if remainingTime < sleepFor {
			sleepFor = remainingTime
		}
		if sleepFor <= 0 {
			break
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(sleepFor):
		}
	}

	err = worker.WorkMany(ctx, jobs)
	multiErr := &MultiError{}
	ok := errors.As(err, &multiErr)
	if !ok {
		multiErr = NewMultiError()
		if err != nil {
			for _, batchJob := range jobs {
				multiErr.AddByID(batchJob.ID, err)
			}
		}
	}

	jobRows := make([]*rivertype.JobRow, 0, len(jobs))
	for _, batchJob := range jobs {
		jobRows = append(jobRows, batchJob.JobRow)
	}
	multiErr.setJobs(jobRows)

	return multiErr
}

func workInProcess[T river.JobArgsWithBatch](ctx context.Context, worker ManyWorker[T], job *river.Job[T], resolvedOpts WorkerOpts) error {
	item := batchItem[T]{job: job, errCh: make(chan error, 1)}
	key := keyForJob(job)

	batchMu.Lock()
	raw, exists := batchGroups[key]
	if !exists {
		batchGroups[key] = &batchGroup[T]{worker: worker, opts: resolvedOpts, items: []batchItem[T]{item}}
		batchMu.Unlock()

		go func() {
			timer := time.NewTimer(resolvedOpts.MaxDelay)
			defer timer.Stop()
			<-timer.C
			flush[T](ctx, key)
		}()

		return <-item.errCh
	}

	group, ok := raw.(*batchGroup[T])
	if !ok {
		batchMu.Unlock()
		return errors.New("batch group type mismatch")
	}
	group.items = append(group.items, item)
	flushNow := len(group.items) >= group.opts.MaxCount
	batchMu.Unlock()

	if flushNow {
		flush[T](ctx, key)
	}

	return <-item.errCh
}

func keyForFetched(job *rivertype.JobRow) string {
	if job == nil {
		return "batch"
	}
	batchKey := job.Kind
	if len(job.Metadata) > 0 && !bytes.Equal(job.Metadata, []byte("{}")) {
		metadata := map[string]json.RawMessage{}
		if json.Unmarshal(job.Metadata, &metadata) == nil {
			if raw, ok := metadata["batch_key"]; ok {
				_ = json.Unmarshal(raw, &batchKey)
			}
		}
	}
	return job.Kind + ":" + batchKey
}

type workManyFunc[T river.JobArgsWithBatch] struct {
	river.WorkerDefaults[T]

	f    func(context.Context, []*river.Job[T]) error
	opts *WorkerOpts
}

func (w *workManyFunc[T]) WorkMany(ctx context.Context, jobs []*river.Job[T]) error {
	return w.f(ctx, jobs)
}

func (w *workManyFunc[T]) Work(ctx context.Context, job *river.Job[T]) error {
	return Work[T](ctx, w, job, w.opts)
}

func WorkFunc[T river.JobArgsWithBatch](f func(context.Context, []*river.Job[T]) error, opts *WorkerOpts) river.Worker[T] {
	if err := validateWorkerOpts(opts); err != nil {
		panic(err)
	}
	return &workManyFunc[T]{f: f, opts: opts}
}

func WorkFuncSafely[T river.JobArgsWithBatch](f func(context.Context, []*river.Job[T]) error, opts *WorkerOpts) (river.Worker[T], error) {
	if f == nil {
		return nil, errors.New("function must not be nil")
	}
	if err := validateWorkerOpts(opts); err != nil {
		return nil, err
	}
	return WorkFunc(f, opts), nil
}
