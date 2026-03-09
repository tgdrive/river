package riverpilot

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/riverqueue/river/internal/notifier"
	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/rivershared/baseservice"
	"github.com/riverqueue/river/rivertype"
)

type StandardPilot struct {
	durablePeriodicJobsEnabled atomic.Bool
	notifyNonTxJobInsert       func(ctx context.Context, res []*rivertype.JobInsertResult)
	seq                        atomic.Int64
}

func (p *StandardPilot) JobCleanerQueuesExcluded() []string { return nil }

func (p *StandardPilot) JobGetAvailable(ctx context.Context, exec riverdriver.Executor, state ProducerState, params *riverdriver.JobGetAvailableParams) ([]*rivertype.JobRow, error) {
	if params.MaxToLock <= 0 {
		return nil, nil
	}

	producerID := int64(0)
	if s, ok := state.(*standardProducerState); ok {
		producerID = s.id
	}

	jobs, err := exec.JobGetAvailableLimited(ctx, &riverdriver.JobGetAvailableLimitedParams{
		AvailablePartitionKeys: params.AvailablePartitionKeys,
		ClientID:               params.ClientID,
		CurrentProducerPartitionKeys: func() []string {
			if s, ok := state.(*standardProducerState); ok {
				keys, _ := s.snapshot()
				return keys
			}
			return nil
		}(),
		CurrentProducerPartitionRunningCounts: func() []int32 {
			if s, ok := state.(*standardProducerState); ok {
				_, counts := s.snapshot()
				return counts
			}
			return nil
		}(),
		GlobalLimit:     params.GlobalLimit,
		LocalLimit:      params.LocalLimit,
		MaxAttemptedBy:  params.MaxAttemptedBy,
		MaxToLock:       params.MaxToLock,
		Now:             params.Now,
		PartitionByArgs: params.PartitionByArgs,
		PartitionByKind: params.PartitionByKind,
		ProducerID:      producerID,
		Queue:           params.Queue,
		Schema:          params.Schema,
	})
	if errors.Is(err, riverdriver.ErrNotImplemented) {
		jobs, err = exec.JobGetAvailable(ctx, &riverdriver.JobGetAvailableParams{
			ClientID:       params.ClientID,
			MaxAttemptedBy: params.MaxAttemptedBy,
			MaxToLock:      params.MaxToLock,
			Now:            params.Now,
			Queue:          params.Queue,
			Schema:         params.Schema,
		})
		if err != nil {
			return nil, err
		}
	}
	if s, ok := state.(*standardProducerState); ok {
		for _, job := range jobs {
			s.JobStart(job)
		}
	}

	return jobs, nil
}

func (p *StandardPilot) JobCancel(ctx context.Context, exec riverdriver.Executor, params *riverdriver.JobCancelParams) (*rivertype.JobRow, error) {
	return exec.JobCancel(ctx, params)
}

func (p *StandardPilot) JobInsertMany(
	ctx context.Context,
	exec riverdriver.Executor,
	params *riverdriver.JobInsertFastManyParams,
) ([]*riverdriver.JobInsertFastResult, error) {
	results, err := exec.JobInsertFastMany(ctx, params)
	if err != nil {
		return nil, err
	}
	if err := appendAndPromoteSequences(ctx, exec, params.Schema, results, params.Jobs, p.notifyNonTxJobInsert); err != nil {
		return nil, err
	}
	return results, nil
}

func (p *StandardPilot) JobRetry(ctx context.Context, exec riverdriver.Executor, params *riverdriver.JobRetryParams) (*rivertype.JobRow, error) {
	return exec.JobRetry(ctx, params)
}

func (p *StandardPilot) JobSetStateIfRunningMany(ctx context.Context, exec riverdriver.Executor, params *riverdriver.JobSetStateIfRunningManyParams) ([]*rivertype.JobRow, error) {
	jobs, err := exec.JobSetStateIfRunningMany(ctx, params)
	if err != nil {
		return nil, err
	}

	var (
		ephemeralIDs   []int64
		sequenceQueues = make(map[string]struct{})
		workflowIDs    = make(map[string]struct{})
	)

	for _, job := range jobs {
		if job == nil {
			continue
		}
		if len(job.Metadata) == 0 || bytes.Equal(job.Metadata, []byte("{}")) {
			continue
		}
		if !bytes.Contains(job.Metadata, []byte("\"ephemeral\"")) && !bytes.Contains(job.Metadata, []byte("\"workflow_id\"")) && !bytes.Contains(job.Metadata, []byte("\"sequence_key\"")) {
			continue
		}

		metadata := map[string]json.RawMessage{}
		_ = json.Unmarshal(job.Metadata, &metadata)

		if job.State == rivertype.JobStateCompleted {
			if ephemeral, ok := metadataBool(metadata, "ephemeral"); ok && ephemeral {
				ephemeralIDs = append(ephemeralIDs, job.ID)
			}
		}

		if isWorkflowFinalizedState(job.State) {
			if workflowID, ok := metadataString(metadata, "workflow_id"); ok && workflowID != "" {
				workflowIDs[workflowID] = struct{}{}
			}
			if sequenceKey, ok := metadataString(metadata, "sequence_key"); ok && sequenceKey != "" {
				sequenceQueues[job.Queue] = struct{}{}
			}
		}
	}

	if len(ephemeralIDs) > 0 {
		if _, err := exec.JobDeleteByIDMany(ctx, &riverdriver.JobDeleteByIDManyParams{ID: ephemeralIDs, Schema: params.Schema}); err != nil && !errors.Is(err, riverdriver.ErrNotImplemented) {
			return nil, err
		}
	}

	for workflowID := range workflowIDs {
		if err := PromoteWorkflowPending(ctx, exec, params.Schema, workflowID); err != nil {
			if errors.Is(err, riverdriver.ErrNotImplemented) {
				continue
			}
			return nil, err
		}
	}

	if err := promoteSequences(ctx, exec, params.Schema, sequenceQueues, p.notifyNonTxJobInsert); err != nil {
		return nil, err
	}

	return jobs, nil
}

func appendAndPromoteSequences(ctx context.Context, exec riverdriver.Executor, schema string, results []*riverdriver.JobInsertFastResult, params []*riverdriver.JobInsertFastParams, notify func(context.Context, []*rivertype.JobInsertResult)) error {
	sequenceAppendByQueue := make(map[string]struct {
		jobIDs []int64
		keys   []string
	})
	sequenceQueues := make(map[string]struct{})

	for i, result := range results {
		if result == nil || result.Job == nil || result.UniqueSkippedAsDuplicate || i >= len(params) {
			continue
		}
		metadata := map[string]json.RawMessage{}
		if err := json.Unmarshal(params[i].Metadata, &metadata); err != nil {
			continue
		}
		sequenceKey, ok := metadataString(metadata, "sequence_key")
		if !ok || sequenceKey == "" {
			continue
		}
		queue := result.Job.Queue
		item := sequenceAppendByQueue[queue]
		item.jobIDs = append(item.jobIDs, result.Job.ID)
		item.keys = append(item.keys, sequenceKey)
		sequenceAppendByQueue[queue] = item
		sequenceQueues[queue] = struct{}{}
	}

	for queue, item := range sequenceAppendByQueue {
		if _, err := exec.SequenceAppendMany(ctx, &riverdriver.SequenceAppendManyParams{JobIDs: item.jobIDs, Keys: item.keys, Queue: queue, Schema: schema}); err != nil && !errors.Is(err, riverdriver.ErrNotImplemented) {
			return err
		}
	}

	return promoteSequences(ctx, exec, schema, sequenceQueues, notify)
}

func promoteSequences(ctx context.Context, exec riverdriver.Executor, schema string, queues map[string]struct{}, notify func(context.Context, []*rivertype.JobInsertResult)) error {
	if len(queues) == 0 {
		return nil
	}
	promotedQueues := make([]string, 0, len(queues))
	for queue := range queues {
		res, err := exec.SequencePromote(ctx, &riverdriver.SequencePromoteParams{Max: 1000, Queue: queue, Schema: schema})
		if err != nil {
			if errors.Is(err, riverdriver.ErrNotImplemented) {
				continue
			}
			return err
		}
		promotedQueues = append(promotedQueues, res...)
	}
	if len(promotedQueues) == 0 {
		return nil
	}
	if notify != nil {
		fake := make([]*rivertype.JobInsertResult, 0, len(promotedQueues))
		for _, queue := range promotedQueues {
			fake = append(fake, &rivertype.JobInsertResult{Job: &rivertype.JobRow{Queue: queue}})
		}
		notify(ctx, fake)
	}
	payloads := make([]string, 0, len(promotedQueues))
	for _, queue := range promotedQueues {
		payloads = append(payloads, fmt.Sprintf("{\"queue\": %q}", queue))
	}
	return exec.NotifyMany(ctx, &riverdriver.NotifyManyParams{Payload: payloads, Schema: schema, Topic: string(notifier.NotificationTopicInsert)})
}

func metadataBool(m map[string]json.RawMessage, key string) (bool, bool) {
	raw, ok := m[key]
	if !ok {
		return false, false
	}
	var val bool
	if err := json.Unmarshal(raw, &val); err != nil {
		return false, false
	}
	return val, true
}

func metadataString(m map[string]json.RawMessage, key string) (string, bool) {
	raw, ok := m[key]
	if !ok {
		return "", false
	}
	var val string
	if err := json.Unmarshal(raw, &val); err != nil {
		return "", false
	}
	return val, true
}

func isWorkflowFinalizedState(state rivertype.JobState) bool {
	switch state {
	case rivertype.JobStateCancelled, rivertype.JobStateCompleted, rivertype.JobStateDiscarded:
		return true
	default:
		return false
	}
}

func metadataBoolDefault(m map[string]json.RawMessage, key string) bool {
	val, ok := metadataBool(m, key)
	return ok && val
}

func PromoteWorkflowPending(ctx context.Context, exec riverdriver.Executor, schema, workflowID string) error {
	rows, err := exec.WorkflowLoadJobsWithDeps(ctx, &riverdriver.WorkflowLoadJobsWithDepsParams{Schema: schema, WorkflowID: workflowID})
	if err != nil {
		return err
	}

	byTask := make(map[string]*riverdriver.WorkflowTaskWithJob, len(rows))
	for _, row := range rows {
		if row == nil || row.Task == nil || row.Job == nil {
			continue
		}
		byTask[row.Task.TaskName] = row
	}

	stageIDs := make([]int64, 0)
	cancelIDs := make([]int64, 0)
	for _, row := range rows {
		if row == nil || row.Task == nil || row.Job == nil || row.Job.State != rivertype.JobStatePending {
			continue
		}

		metadata := map[string]json.RawMessage{}
		if len(row.Job.Metadata) > 0 {
			_ = json.Unmarshal(row.Job.Metadata, &metadata)
		}

		ignoreCancelledDeps := metadataBoolDefault(metadata, "workflow_ignore_cancelled_deps")
		ignoreDiscardedDeps := metadataBoolDefault(metadata, "workflow_ignore_discarded_deps")
		ignoreDeletedDeps := metadataBoolDefault(metadata, "workflow_ignore_deleted_deps")

		depsSatisfied := true
		shouldCancel := false
		for _, dep := range row.Task.Deps {
			depRow, ok := byTask[dep]
			if !ok || depRow == nil || depRow.Job == nil {
				if ignoreDeletedDeps {
					continue
				}
				shouldCancel = true
				depsSatisfied = false
				break
			}

			switch depRow.Job.State {
			case rivertype.JobStateCompleted:
				continue
			case rivertype.JobStateCancelled:
				if ignoreCancelledDeps {
					continue
				}
				shouldCancel = true
				depsSatisfied = false
			case rivertype.JobStateDiscarded:
				if ignoreDiscardedDeps {
					continue
				}
				shouldCancel = true
				depsSatisfied = false
			default:
				depsSatisfied = false
			}

			if shouldCancel || !depsSatisfied {
				break
			}
		}

		if shouldCancel {
			cancelIDs = append(cancelIDs, row.Job.ID)
			continue
		}

		if depsSatisfied {
			stageIDs = append(stageIDs, row.Job.ID)
		}
	}

	if len(cancelIDs) > 0 {
		if _, err := exec.WorkflowCancelWithFailedDepsMany(ctx, &riverdriver.WorkflowCancelWithFailedDepsManyParams{JobID: cancelIDs, Schema: schema}); err != nil {
			return err
		}
	}

	if len(stageIDs) == 0 {
		return nil
	}

	_, err = exec.WorkflowStageJobsByIDMany(ctx, &riverdriver.WorkflowStageJobsByIDManyParams{ID: stageIDs, Schema: schema})
	return err
}

func (p *StandardPilot) PeriodicJobKeepAliveAndReap(ctx context.Context, exec riverdriver.Executor, params *PeriodicJobKeepAliveAndReapParams) ([]*PeriodicJob, error) {
	if !p.durablePeriodicJobsEnabled.Load() {
		return nil, nil
	}

	now := time.Now().UTC()

	staleThreshold := params.StaleThreshold
	if staleThreshold == 0 {
		staleThreshold = 24 * time.Hour
	}

	// Reap stale periodic jobs first.
	reaped, err := exec.PeriodicJobKeepAliveAndReap(ctx, &riverdriver.PeriodicJobKeepAliveAndReapParams{
		Now:                   &now,
		Schema:                params.Schema,
		StaleUpdatedAtHorizon: now.Add(-staleThreshold),
	})
	if errors.Is(err, riverdriver.ErrNotImplemented) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	if len(params.ID) > 0 {
		all, err := exec.PeriodicJobGetAll(ctx, &riverdriver.PeriodicJobGetAllParams{Schema: params.Schema})
		if err != nil {
			return nil, err
		}
		idsByName := make(map[string]int64, len(all))
		for _, periodicJob := range all {
			idsByName[periodicJob.Name] = periodicJob.ID
		}

		for _, name := range params.ID {
			id, ok := idsByName[name]
			if !ok {
				continue
			}
			if _, err := exec.PeriodicJobKeepAliveAndReap(ctx, &riverdriver.PeriodicJobKeepAliveAndReapParams{
				ID:                    id,
				Now:                   &now,
				Schema:                params.Schema,
				StaleUpdatedAtHorizon: time.Time{},
			}); err != nil {
				return nil, err
			}
		}
	}

	return periodicJobsFromDriver(reaped), nil
}

func (p *StandardPilot) PeriodicJobGetAll(ctx context.Context, exec riverdriver.Executor, params *PeriodicJobGetAllParams) ([]*PeriodicJob, error) {
	if !p.durablePeriodicJobsEnabled.Load() {
		return nil, nil
	}

	jobs, err := exec.PeriodicJobGetAll(ctx, &riverdriver.PeriodicJobGetAllParams{Schema: params.Schema})
	if errors.Is(err, riverdriver.ErrNotImplemented) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return periodicJobsFromDriver(jobs), nil
}

func (p *StandardPilot) PeriodicJobUpsertMany(ctx context.Context, exec riverdriver.Executor, params *PeriodicJobUpsertManyParams) ([]*PeriodicJob, error) {
	if !p.durablePeriodicJobsEnabled.Load() {
		return nil, nil
	}

	jobs := make([]*riverdriver.PeriodicJobInsertParams, 0, len(params.Jobs))
	for _, job := range params.Jobs {
		jobs = append(jobs, &riverdriver.PeriodicJobInsertParams{
			Metadata:  []byte("{}"),
			Name:      job.ID,
			NextRunAt: job.NextRunAt,
			Queue:     "",
			Schema:    params.Schema,
		})
	}

	upserted, err := exec.PeriodicJobUpsertMany(ctx, &riverdriver.PeriodicJobUpsertManyParams{
		PeriodicJobs: jobs,
		Schema:       params.Schema,
	})
	if errors.Is(err, riverdriver.ErrNotImplemented) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return periodicJobsFromDriver(upserted), nil
}

func (p *StandardPilot) PilotInit(archetype *baseservice.Archetype, params *PilotInitParams) {
	p.durablePeriodicJobsEnabled.Store(params.DurablePeriodicJobsEnabled)
	p.notifyNonTxJobInsert = params.NotifyNonTxJobInsert
}

func (p *StandardPilot) ProducerInit(ctx context.Context, exec riverdriver.Executor, params *ProducerInitParams) (int64, ProducerState, error) {
	id := params.ProducerID
	if id == 0 {
		id = p.seq.Add(1)
	}

	_, err := exec.ProducerInsertOrUpdate(ctx, &riverdriver.ProducerInsertOrUpdateParams{
		ID:        id,
		Metadata:  []byte("{}"),
		Now:       ptrTime(time.Now().UTC()),
		QueueName: params.Queue,
		Schema:    params.Schema,
	})
	if err != nil && !errors.Is(err, riverdriver.ErrNotImplemented) {
		return 0, nil, err
	}

	return id, &standardProducerState{id: id, jobPartition: make(map[int64]string), runningCounts: make(map[string]int32)}, nil
}

func (p *StandardPilot) ProducerKeepAlive(ctx context.Context, exec riverdriver.Executor, params *riverdriver.ProducerKeepAliveParams) error {
	_, err := exec.ProducerKeepAlive(ctx, params)
	if errors.Is(err, riverdriver.ErrNotImplemented) {
		return nil
	}
	return err
}

func (p *StandardPilot) ProducerShutdown(ctx context.Context, exec riverdriver.Executor, params *ProducerShutdownParams) error {
	err := exec.ProducerDelete(ctx, &riverdriver.ProducerDeleteParams{
		ID:     params.ProducerID,
		Schema: params.Schema,
	})
	if errors.Is(err, riverdriver.ErrNotImplemented) {
		return nil
	}
	return err
}

func (p *StandardPilot) QueueMetadataChanged(ctx context.Context, exec riverdriver.Executor, params *QueueMetadataChangedParams) error {
	producers, err := exec.ProducerListByQueue(ctx, &riverdriver.ProducerListByQueueParams{
		QueueName: params.Queue,
		Schema:    params.Schema,
	})
	if errors.Is(err, riverdriver.ErrNotImplemented) {
		return nil
	}
	if err != nil {
		return err
	}

	for _, producer := range producers {
		if _, err := exec.ProducerUpdate(ctx, &riverdriver.ProducerUpdateParams{
			ID:       producer.ID,
			Metadata: params.Metadata,
			Schema:   params.Schema,
		}); err != nil && !errors.Is(err, riverdriver.ErrNotImplemented) {
			return err
		}
	}

	return nil
}

type standardProducerState struct {
	id            int64
	mu            sync.Mutex
	jobPartition  map[int64]string
	runningCounts map[string]int32
}

func (s *standardProducerState) JobStart(job *rivertype.JobRow) {
	partitionKey := job.Kind
	s.mu.Lock()
	defer s.mu.Unlock()
	s.jobPartition[job.ID] = partitionKey
	s.runningCounts[partitionKey]++
}

func (s *standardProducerState) snapshot() ([]string, []int32) {
	s.mu.Lock()
	defer s.mu.Unlock()
	keys := make([]string, 0, len(s.runningCounts))
	counts := make([]int32, 0, len(s.runningCounts))
	for key, count := range s.runningCounts {
		keys = append(keys, key)
		counts = append(counts, count)
	}
	return keys, counts
}

func periodicJobsFromDriver(jobs []*riverdriver.PeriodicJob) []*PeriodicJob {
	out := make([]*PeriodicJob, 0, len(jobs))
	for _, job := range jobs {
		out = append(out, &PeriodicJob{
			ID:        job.Name,
			CreatedAt: job.CreatedAt,
			NextRunAt: job.NextRunAt,
			UpdatedAt: job.UpdatedAt,
		})
	}
	return out
}

func ptrTime(t time.Time) *time.Time { return &t }

func (s *standardProducerState) JobFinish(job *rivertype.JobRow) {
	s.mu.Lock()
	defer s.mu.Unlock()
	partitionKey, ok := s.jobPartition[job.ID]
	if !ok {
		return
	}
	delete(s.jobPartition, job.ID)
	if s.runningCounts[partitionKey] <= 1 {
		delete(s.runningCounts, partitionKey)
		return
	}
	s.runningCounts[partitionKey]--
}
