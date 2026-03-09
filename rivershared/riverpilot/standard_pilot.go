package riverpilot

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"sync/atomic"
	"time"

	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/rivershared/baseservice"
	"github.com/riverqueue/river/rivertype"
)

type StandardPilot struct {
	durablePeriodicJobsEnabled atomic.Bool
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
		ClientID:        params.ClientID,
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
	return exec.JobInsertFastMany(ctx, params)
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
		ephemeralIDs []int64
		workflowIDs  = make(map[string]struct{})
	)

	for _, job := range jobs {
		if job == nil {
			continue
		}
		if job.State != rivertype.JobStateCompleted {
			continue
		}
		if len(job.Metadata) == 0 || bytes.Equal(job.Metadata, []byte("{}")) {
			continue
		}
		if !bytes.Contains(job.Metadata, []byte("\"ephemeral\"")) && !bytes.Contains(job.Metadata, []byte("\"workflow_id\"")) {
			continue
		}

		metadata := map[string]json.RawMessage{}
		_ = json.Unmarshal(job.Metadata, &metadata)

		if ephemeral, ok := metadataBool(metadata, "ephemeral"); ok && ephemeral {
			ephemeralIDs = append(ephemeralIDs, job.ID)
		}

		if workflowID, ok := metadataString(metadata, "workflow_id"); ok && workflowID != "" {
			workflowIDs[workflowID] = struct{}{}
		}
	}

	if len(ephemeralIDs) > 0 {
		if _, err := exec.JobDeleteByIDMany(ctx, &riverdriver.JobDeleteByIDManyParams{ID: ephemeralIDs, Schema: params.Schema}); err != nil && !errors.Is(err, riverdriver.ErrNotImplemented) {
			return nil, err
		}
	}

	for workflowID := range workflowIDs {
		if err := promoteWorkflowPending(ctx, exec, params.Schema, workflowID); err != nil {
			if errors.Is(err, riverdriver.ErrNotImplemented) {
				continue
			}
			return nil, err
		}
	}

	return jobs, nil
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

func promoteWorkflowPending(ctx context.Context, exec riverdriver.Executor, schema, workflowID string) error {
	rows, err := exec.WorkflowLoadJobsWithDeps(ctx, &riverdriver.WorkflowLoadJobsWithDepsParams{Schema: schema, WorkflowID: workflowID})
	if err != nil {
		return err
	}

	completed := make(map[string]struct{}, len(rows))
	for _, row := range rows {
		if row == nil || row.Task == nil || row.Job == nil {
			continue
		}
		if row.Job.State == rivertype.JobStateCompleted {
			completed[row.Task.TaskName] = struct{}{}
		}
	}

	stageIDs := make([]int64, 0)
	for _, row := range rows {
		if row == nil || row.Task == nil || row.Job == nil || row.Job.State != rivertype.JobStatePending {
			continue
		}

		depsSatisfied := true
		for _, dep := range row.Task.Deps {
			if _, ok := completed[dep]; !ok {
				depsSatisfied = false
				break
			}
		}

		if depsSatisfied {
			stageIDs = append(stageIDs, row.Job.ID)
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

	// Reap stale periodic jobs first.
	reaped, err := exec.PeriodicJobKeepAliveAndReap(ctx, &riverdriver.PeriodicJobKeepAliveAndReapParams{
		Now:                   &now,
		Schema:                params.Schema,
		StaleUpdatedAtHorizon: now.Add(-24 * time.Hour),
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

	return id, &standardProducerState{id: id}, nil
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
	id int64
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
	// No-op
}
