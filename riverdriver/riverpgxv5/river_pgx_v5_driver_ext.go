package riverpgxv5

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgtype"

	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/riverdriver/riverpgxv5/internal/dbsqlc"
	"github.com/riverqueue/river/rivershared/uniquestates"
	"github.com/riverqueue/river/rivershared/util/hashutil"
	"github.com/riverqueue/river/rivertype"
)

var errMismatchedSequenceAppendInput = errors.New("sequence append input lengths must match")

func schemaPrefix(schema string) string {
	if schema == "" {
		return ""
	}
	return schema + "."
}

func marshalAttemptErrors(errorsIn []rivertype.AttemptError) ([][]byte, error) {
	errs := make([][]byte, 0, len(errorsIn))
	for _, e := range errorsIn {
		b, err := json.Marshal(e)
		if err != nil {
			return nil, err
		}
		errs = append(errs, b)
	}
	return errs, nil
}

func (e *Executor) fetchJobsByIDMany(ctx context.Context, schema string, ids []int64) ([]*rivertype.JobRow, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	jobs, err := e.JobGetByIDMany(ctx, &riverdriver.JobGetByIDManyParams{ID: ids, Schema: schema})
	if err != nil {
		return nil, err
	}
	return jobs, nil
}

func (e *Executor) JobDeadLetterDeleteByID(ctx context.Context, params *riverdriver.JobDeadLetterDeleteByIDParams) (*rivertype.JobRow, error) {
	raw, err := dbsqlc.New().JobDeadLetterDeleteByID(schemaTemplateParam(ctx, params.Schema), e.dbtx, params.ID)
	if err != nil {
		return nil, interpretError(err)
	}
	job := &rivertype.JobRow{}
	if err := json.Unmarshal(raw, job); err != nil {
		return nil, err
	}
	return job, nil
}

func (e *Executor) JobDeadLetterGetAll(ctx context.Context, params *riverdriver.JobDeadLetterGetAllParams) ([]*rivertype.JobRow, error) {
	raws, err := dbsqlc.New().JobDeadLetterGetAll(schemaTemplateParam(ctx, params.Schema), e.dbtx, int32(params.Max))
	if err != nil {
		return nil, interpretError(err)
	}

	jobs := make([]*rivertype.JobRow, 0, len(raws))
	for _, raw := range raws {
		job := &rivertype.JobRow{}
		if err := json.Unmarshal(raw, job); err != nil {
			return nil, err
		}
		jobs = append(jobs, job)
	}
	return jobs, nil
}

func (e *Executor) JobDeadLetterGetByID(ctx context.Context, params *riverdriver.JobDeadLetterGetByIDParams) (*rivertype.JobRow, error) {
	raw, err := dbsqlc.New().JobDeadLetterGetByID(schemaTemplateParam(ctx, params.Schema), e.dbtx, params.ID)
	if err != nil {
		return nil, interpretError(err)
	}
	job := &rivertype.JobRow{}
	if err := json.Unmarshal(raw, job); err != nil {
		return nil, err
	}
	return job, nil
}

func (e *Executor) JobDeadLetterMoveByID(ctx context.Context, params *riverdriver.JobDeadLetterMoveByIDParams) (*rivertype.JobRow, error) {
	job, err := e.JobDeadLetterGetByID(ctx, &riverdriver.JobDeadLetterGetByIDParams{ID: params.ID, Schema: params.Schema})
	if err != nil {
		return nil, err
	}

	now := time.Now().UTC()

	// Reset attempts and errors so the retried job gets a fresh lifecycle.
	job.Attempt = 0
	job.AttemptedAt = nil
	job.AttemptedBy = nil
	job.Errors = nil
	job.State = rivertype.JobStateAvailable
	job.FinalizedAt = nil
	job.ScheduledAt = now

	errData, err := marshalAttemptErrors(job.Errors)
	if err != nil {
		return nil, err
	}

	inserted, err := e.JobInsertFull(ctx, &riverdriver.JobInsertFullParams{
		Attempt:      job.Attempt,
		AttemptedAt:  job.AttemptedAt,
		AttemptedBy:  job.AttemptedBy,
		CreatedAt:    &job.CreatedAt,
		EncodedArgs:  job.EncodedArgs,
		Errors:       errData,
		FinalizedAt:  nil,
		Kind:         job.Kind,
		MaxAttempts:  job.MaxAttempts,
		Metadata:     job.Metadata,
		Priority:     job.Priority,
		Queue:        job.Queue,
		ScheduledAt:  &job.ScheduledAt,
		Schema:       params.Schema,
		State:        rivertype.JobStateAvailable,
		Tags:         job.Tags,
		UniqueKey:    job.UniqueKey,
		UniqueStates: uniquestates.UniqueStatesToBitmask(job.UniqueStates),
	})
	if err != nil {
		return nil, err
	}

	if _, err := e.JobDeadLetterDeleteByID(ctx, &riverdriver.JobDeadLetterDeleteByIDParams{ID: params.ID, Schema: params.Schema}); err != nil {
		return nil, err
	}

	return inserted, nil
}

func (e *Executor) JobDeadLetterMoveDiscarded(ctx context.Context, params *riverdriver.JobDeadLetterMoveDiscardedParams) ([]*rivertype.JobRow, error) {
	jobs, err := e.JobDeleteMany(ctx, &riverdriver.JobDeleteManyParams{
		Max:           int32(params.Max),
		OrderByClause: "finalized_at ASC, id ASC",
		Schema:        params.Schema,
		WhereClause:   "state = 'discarded' AND finalized_at <= @discarded_finalized_at_horizon",
		NamedArgs: map[string]any{
			"discarded_finalized_at_horizon": params.DiscardedFinalizedAtHorizon,
		},
	})
	if err != nil {
		return nil, err
	}

	for _, job := range jobs {
		raw, err := json.Marshal(job)
		if err != nil {
			return nil, err
		}
		if err := dbsqlc.New().JobDeadLetterUpsert(schemaTemplateParam(ctx, params.Schema), e.dbtx, &dbsqlc.JobDeadLetterUpsertParams{ID: job.ID, Job: raw}); err != nil {
			return nil, interpretError(err)
		}
	}

	return jobs, nil
}

func (e *Executor) JobDeleteByIDMany(ctx context.Context, params *riverdriver.JobDeleteByIDManyParams) ([]*rivertype.JobRow, error) {
	preDeleteJobs, err := e.fetchJobsByIDMany(ctx, params.Schema, params.ID)
	if err != nil {
		return nil, err
	}

	deletedIDs, err := dbsqlc.New().JobDeleteByIDManyReturningIDs(schemaTemplateParam(ctx, params.Schema), e.dbtx, params.ID)
	if err != nil {
		return nil, interpretError(err)
	}

	deletedSet := make(map[int64]struct{}, len(deletedIDs))
	for _, id := range deletedIDs {
		deletedSet[id] = struct{}{}
	}

	deletedJobs := make([]*rivertype.JobRow, 0, len(deletedIDs))
	for _, job := range preDeleteJobs {
		if _, ok := deletedSet[job.ID]; ok {
			deletedJobs = append(deletedJobs, job)
		}
	}

	return deletedJobs, nil
}

func (e *Executor) JobGetAvailableForBatch(ctx context.Context, params *riverdriver.JobGetAvailableForBatchParams) ([]*rivertype.JobRow, error) {
	ids, err := dbsqlc.New().JobGetAvailableForBatch(schemaTemplateParam(ctx, params.Schema), e.dbtx, &dbsqlc.JobGetAvailableForBatchParams{
		AttemptedBy: params.ClientID,
		BatchKey:    pgtype.Text{String: params.BatchKey, Valid: params.BatchKey != ""},
		Kind:        params.Kind,
		MaxToLock:   int32(params.MaxToLock),
		Now:         params.Now,
		Queue:       params.Queue,
	})
	if err != nil {
		return nil, interpretError(err)
	}

	return e.fetchJobsByIDMany(ctx, params.Schema, ids)
}

func (e *Executor) JobGetAvailableLimited(ctx context.Context, params *riverdriver.JobGetAvailableLimitedParams) ([]*rivertype.JobRow, error) {
	globalLimit := int(params.GlobalLimit)
	localLimit := int(params.LocalLimit)
	if globalLimit <= 0 {
		globalLimit = 1_000_000
	}
	if localLimit <= 0 {
		localLimit = 1_000_000
	}

	if globalLimit >= 1_000_000 && localLimit >= 1_000_000 {
		return e.JobGetAvailable(ctx, &riverdriver.JobGetAvailableParams{
			ClientID:       params.ClientID,
			MaxAttemptedBy: params.MaxAttemptedBy,
			MaxToLock:      params.MaxToLock,
			Now:            params.Now,
			Queue:          params.Queue,
			Schema:         params.Schema,
		})
	}

	idsTyped, err := dbsqlc.New().JobGetAvailableLimited(schemaTemplateParam(ctx, params.Schema), e.dbtx, &dbsqlc.JobGetAvailableLimitedParams{
		Queue:                                 params.Queue,
		Now:                                   params.Now,
		GlobalLimit:                           int32(globalLimit),
		LocalLimit:                            int32(localLimit),
		MaxToLock:                             int32(params.MaxToLock),
		MaxAttemptedBy:                        int32(params.MaxAttemptedBy),
		AttemptedBy:                           params.ClientID,
		AvailablePartitionKeys:                params.AvailablePartitionKeys,
		CurrentProducerPartitionKeys:          params.CurrentProducerPartitionKeys,
		CurrentProducerPartitionRunningCounts: params.CurrentProducerPartitionRunningCounts,
		PartitionByArgs:                       params.PartitionByArgs,
		PartitionByKind:                       params.PartitionByKind,
	})
	if err != nil {
		return nil, interpretError(err)
	}

	ids := make([]int64, 0, len(idsTyped))
	for _, id := range idsTyped {
		if !id.Valid {
			continue
		}
		ids = append(ids, id.Int64)
	}

	return e.fetchJobsByIDMany(ctx, params.Schema, ids)
}

func (e *Executor) JobGetAvailablePartitionKeys(ctx context.Context, params *riverdriver.JobGetAvailablePartitionKeysParams) ([]string, error) {
	keys, err := dbsqlc.New().JobGetAvailablePartitionKeys(schemaTemplateParam(ctx, params.Schema), e.dbtx, &dbsqlc.JobGetAvailablePartitionKeysParams{
		Queue:           params.Queue,
		Now:             nil,
		PartitionByArgs: params.PartitionByArgs,
		PartitionByKind: params.PartitionByKind,
	})
	if err != nil {
		return nil, interpretError(err)
	}
	out := make([]string, 0, len(keys))
	for _, key := range keys {
		out = append(out, anyToString(key))
	}
	return out, nil
}

func (e *Executor) PGTryAdvisoryXactLock(ctx context.Context, key int64) (bool, error) {
	ok, err := dbsqlc.New().PGTryAdvisoryXactLock(ctx, e.dbtx, key)
	if err != nil {
		return false, interpretError(err)
	}
	return ok, nil
}

func (e *Executor) PeriodicJobGetAll(ctx context.Context, params *riverdriver.PeriodicJobGetAllParams) ([]*riverdriver.PeriodicJob, error) {
	rows, err := dbsqlc.New().PeriodicJobGetAllExt(schemaTemplateParam(ctx, params.Schema), e.dbtx)
	if err != nil {
		return nil, interpretError(err)
	}
	out := make([]*riverdriver.PeriodicJob, 0, len(rows))
	for _, row := range rows {
		out = append(out, &riverdriver.PeriodicJob{ID: row.ID, CreatedAt: row.CreatedAt, UpdatedAt: row.UpdatedAt, Name: row.Name, Queue: row.Queue, NextRunAt: row.NextRunAt, Metadata: row.Metadata})
	}
	return out, nil
}

func (e *Executor) PeriodicJobGetByID(ctx context.Context, params *riverdriver.PeriodicJobGetByIDParams) (*riverdriver.PeriodicJob, error) {
	row, err := dbsqlc.New().PeriodicJobGetByIDExt(schemaTemplateParam(ctx, params.Schema), e.dbtx, params.ID)
	if err != nil {
		return nil, interpretError(err)
	}
	return &riverdriver.PeriodicJob{ID: row.ID, CreatedAt: row.CreatedAt, UpdatedAt: row.UpdatedAt, Name: row.Name, Queue: row.Queue, NextRunAt: row.NextRunAt, Metadata: row.Metadata}, nil
}

func (e *Executor) PeriodicJobInsert(ctx context.Context, params *riverdriver.PeriodicJobInsertParams) (*riverdriver.PeriodicJob, error) {
	row, err := dbsqlc.New().PeriodicJobInsertExt(schemaTemplateParam(ctx, params.Schema), e.dbtx, &dbsqlc.PeriodicJobInsertExtParams{Name: params.Name, Queue: params.Queue, NextRunAt: params.NextRunAt, Metadata: params.Metadata})
	if err != nil {
		return nil, interpretError(err)
	}
	return &riverdriver.PeriodicJob{ID: row.ID, CreatedAt: row.CreatedAt, UpdatedAt: row.UpdatedAt, Name: row.Name, Queue: row.Queue, NextRunAt: row.NextRunAt, Metadata: row.Metadata}, nil
}

func (e *Executor) PeriodicJobKeepAliveAndReap(ctx context.Context, params *riverdriver.PeriodicJobKeepAliveAndReapParams) ([]*riverdriver.PeriodicJob, error) {
	reapedRows, err := dbsqlc.New().PeriodicJobDeleteStale(schemaTemplateParam(ctx, params.Schema), e.dbtx, params.StaleUpdatedAtHorizon)
	if err != nil {
		return nil, interpretError(err)
	}
	reaped := make([]*riverdriver.PeriodicJob, 0, len(reapedRows))
	for _, row := range reapedRows {
		reaped = append(reaped, &riverdriver.PeriodicJob{ID: row.ID, CreatedAt: row.CreatedAt, UpdatedAt: row.UpdatedAt, Name: row.Name, Queue: row.Queue, NextRunAt: row.NextRunAt, Metadata: row.Metadata})
	}

	if params.ID != 0 {
		if err := dbsqlc.New().PeriodicJobTouchByID(schemaTemplateParam(ctx, params.Schema), e.dbtx, &dbsqlc.PeriodicJobTouchByIDParams{ID: params.ID, Now: params.Now}); err != nil {
			return nil, interpretError(err)
		}
	}

	return reaped, nil
}

func (e *Executor) PeriodicJobUpsertMany(ctx context.Context, params *riverdriver.PeriodicJobUpsertManyParams) ([]*riverdriver.PeriodicJob, error) {
	out := make([]*riverdriver.PeriodicJob, 0, len(params.PeriodicJobs))
	for _, p := range params.PeriodicJobs {
		row, err := dbsqlc.New().PeriodicJobUpsertExt(schemaTemplateParam(ctx, params.Schema), e.dbtx, &dbsqlc.PeriodicJobUpsertExtParams{Name: p.Name, Queue: p.Queue, NextRunAt: p.NextRunAt, Metadata: p.Metadata})
		if err != nil {
			return nil, interpretError(err)
		}
		out = append(out, &riverdriver.PeriodicJob{ID: row.ID, CreatedAt: row.CreatedAt, UpdatedAt: row.UpdatedAt, Name: row.Name, Queue: row.Queue, NextRunAt: row.NextRunAt, Metadata: row.Metadata})
	}
	return out, nil
}

func (e *Executor) ProducerDelete(ctx context.Context, params *riverdriver.ProducerDeleteParams) error {
	err := dbsqlc.New().ProducerDeleteExt(schemaTemplateParam(ctx, params.Schema), e.dbtx, params.ID)
	return interpretError(err)
}

func (e *Executor) ProducerGetByID(ctx context.Context, params *riverdriver.ProducerGetByIDParams) (*riverdriver.Producer, error) {
	row, err := dbsqlc.New().ProducerGetByIDExt(schemaTemplateParam(ctx, params.Schema), e.dbtx, params.ID)
	if err != nil {
		return nil, interpretError(err)
	}
	return &riverdriver.Producer{ID: row.ID, CreatedAt: row.CreatedAt, UpdatedAt: row.UpdatedAt, QueueName: row.QueueName, Metadata: row.Metadata}, nil
}

func (e *Executor) ProducerInsertOrUpdate(ctx context.Context, params *riverdriver.ProducerInsertOrUpdateParams) (*riverdriver.Producer, error) {
	row, err := dbsqlc.New().ProducerInsertOrUpdateExt(schemaTemplateParam(ctx, params.Schema), e.dbtx, &dbsqlc.ProducerInsertOrUpdateExtParams{
		ID:        params.ID,
		QueueName: params.QueueName,
		Metadata:  params.Metadata,
		Now:       params.Now,
	})
	if err != nil {
		return nil, interpretError(err)
	}
	return &riverdriver.Producer{ID: row.ID, CreatedAt: row.CreatedAt, UpdatedAt: row.UpdatedAt, QueueName: row.QueueName, Metadata: row.Metadata}, nil
}

func (e *Executor) ProducerKeepAlive(ctx context.Context, params *riverdriver.ProducerKeepAliveParams) (*riverdriver.Producer, error) {
	if err := dbsqlc.New().ProducerDeleteStaleExcludingID(schemaTemplateParam(ctx, params.Schema), e.dbtx, &dbsqlc.ProducerDeleteStaleExcludingIDParams{StaleUpdatedAtHorizon: params.StaleUpdatedAtHorizon, ID: params.ID}); err != nil {
		return nil, interpretError(err)
	}
	row, err := dbsqlc.New().ProducerKeepAliveUpsert(schemaTemplateParam(ctx, params.Schema), e.dbtx, &dbsqlc.ProducerKeepAliveUpsertParams{ID: params.ID, QueueName: params.QueueName})
	if err != nil {
		return nil, interpretError(err)
	}
	return &riverdriver.Producer{ID: row.ID, CreatedAt: row.CreatedAt, UpdatedAt: row.UpdatedAt, QueueName: row.QueueName, Metadata: row.Metadata}, nil
}

func (e *Executor) ProducerListByQueue(ctx context.Context, params *riverdriver.ProducerListByQueueParams) ([]*riverdriver.ProducerListByQueueResult, error) {
	rows, err := dbsqlc.New().ProducerListByQueueExt(schemaTemplateParam(ctx, params.Schema), e.dbtx, params.QueueName)
	if err != nil {
		return nil, interpretError(err)
	}

	out := make([]*riverdriver.ProducerListByQueueResult, 0, len(rows))
	for _, row := range rows {
		out = append(out, &riverdriver.ProducerListByQueueResult{ID: row.ID, QueueName: row.QueueName, Metadata: row.Metadata, UpdatedAt: row.UpdatedAt})
	}
	return out, nil
}

func (e *Executor) ProducerUpdate(ctx context.Context, params *riverdriver.ProducerUpdateParams) (*riverdriver.Producer, error) {
	row, err := dbsqlc.New().ProducerUpdateExt(schemaTemplateParam(ctx, params.Schema), e.dbtx, &dbsqlc.ProducerUpdateExtParams{ID: params.ID, Metadata: params.Metadata, UpdatedAt: params.UpdatedAt})
	if err != nil {
		return nil, interpretError(err)
	}
	return &riverdriver.Producer{ID: row.ID, CreatedAt: row.CreatedAt, UpdatedAt: row.UpdatedAt, QueueName: row.QueueName, Metadata: row.Metadata}, nil
}

func (e *Executor) QueueGetMetadataForInsert(ctx context.Context, params *riverdriver.QueueGetMetadataForInsertParams) ([]*riverdriver.QueueGetMetadataForInsertResult, error) {
	if len(params.QueueNames) == 0 {
		return nil, nil
	}
	rows, err := dbsqlc.New().QueueGetMetadataForInsert(schemaTemplateParam(ctx, params.Schema), e.dbtx, params.QueueNames)
	if err != nil {
		return nil, interpretError(err)
	}
	out := make([]*riverdriver.QueueGetMetadataForInsertResult, 0, len(rows))
	for _, row := range rows {
		out = append(out, &riverdriver.QueueGetMetadataForInsertResult{Name: row.Name, Metadata: row.Metadata})
	}
	return out, nil
}

func (e *Executor) SequenceAppendMany(ctx context.Context, params *riverdriver.SequenceAppendManyParams) (int, error) {
	if len(params.JobIDs) != len(params.Keys) {
		return 0, errMismatchedSequenceAppendInput
	}

	count := 0
	for i := range params.JobIDs {
		if err := dbsqlc.New().SequenceAppend(schemaTemplateParam(ctx, params.Schema), e.dbtx, &dbsqlc.SequenceAppendParams{Queue: params.Queue, Key: params.Keys[i], LatestJobID: params.JobIDs[i]}); err != nil {
			return count, interpretError(err)
		}
		count++
	}
	return count, nil
}

func (e *Executor) SequenceList(ctx context.Context, params *riverdriver.SequenceListParams) ([]*riverdriver.Sequence, error) {
	rows, err := dbsqlc.New().SequenceListExt(schemaTemplateParam(ctx, params.Schema), e.dbtx, &dbsqlc.SequenceListExtParams{Queue: params.Queue, Max: int32(params.Max)})
	if err != nil {
		return nil, interpretError(err)
	}
	out := make([]*riverdriver.Sequence, 0, len(rows))
	for _, row := range rows {
		out = append(out, &riverdriver.Sequence{ID: row.ID, CreatedAt: row.CreatedAt, UpdatedAt: row.UpdatedAt, Queue: row.Queue, Key: row.Key, LatestJobID: row.LatestJobID})
	}
	return out, nil
}

func (e *Executor) SequencePromote(ctx context.Context, params *riverdriver.SequencePromoteParams) ([]string, error) {
	rows, err := dbsqlc.New().SequencePromote(schemaTemplateParam(ctx, params.Schema), e.dbtx, &dbsqlc.SequencePromoteParams{Queue: params.Queue, Max: int32(params.Max)})
	if err != nil {
		return nil, interpretError(err)
	}
	queuesSet := map[string]struct{}{}
	for _, queue := range rows {
		queuesSet[queue] = struct{}{}
	}
	queues := make([]string, 0, len(queuesSet))
	for q := range queuesSet {
		queues = append(queues, q)
	}
	sort.Strings(queues)
	return queues, nil
}

func (e *Executor) SequencePromoteFromTable(ctx context.Context, params *riverdriver.SequencePromoteFromTableParams) (*riverdriver.SequencePromoteFromTableResult, error) {
	queues, err := dbsqlc.New().SequencePromoteFromTable(schemaTemplateParam(ctx, params.Schema), e.dbtx, int32(params.Max))
	if err != nil {
		return nil, interpretError(err)
	}
	return &riverdriver.SequencePromoteFromTableResult{PromotedQueues: queues}, nil
}

func (e *Executor) SequenceScanAndPromoteStalled(ctx context.Context, params *riverdriver.SequenceScanAndPromoteStalledParams) (*riverdriver.SequenceScanAndPromoteStalledResult, error) {
	res, err := e.SequencePromoteFromTable(ctx, &riverdriver.SequencePromoteFromTableParams{Max: params.Max, Schema: params.Schema})
	if err != nil {
		return nil, err
	}
	return &riverdriver.SequenceScanAndPromoteStalledResult{PromotedQueues: res.PromotedQueues}, nil
}

func (e *Executor) WorkflowCancel(ctx context.Context, params *riverdriver.WorkflowCancelParams) ([]*rivertype.JobRow, error) {
	ids, err := dbsqlc.New().WorkflowCancelByWorkflowID(schemaTemplateParam(ctx, params.Schema), e.dbtx, pgtype.Text{String: params.WorkflowID, Valid: params.WorkflowID != ""})
	if err != nil {
		return nil, interpretError(err)
	}
	return e.fetchJobsByIDMany(ctx, params.Schema, ids)
}

func (e *Executor) WorkflowCancelWithFailedDepsMany(ctx context.Context, params *riverdriver.WorkflowCancelWithFailedDepsManyParams) ([]*rivertype.JobRow, error) {
	if len(params.JobID) == 0 {
		return nil, nil
	}
	failedAt := params.WorkflowDepsFailedAt
	if failedAt.IsZero() {
		failedAt = time.Now().UTC()
	}
	ids, err := dbsqlc.New().WorkflowCancelWithFailedDepsByIDs(schemaTemplateParam(ctx, params.Schema), e.dbtx, &dbsqlc.WorkflowCancelWithFailedDepsByIDsParams{
		WorkflowDepsFailedAt: &failedAt,
		IDs:                  params.JobID,
	})
	if err != nil {
		return nil, interpretError(err)
	}
	return e.fetchJobsByIDMany(ctx, params.Schema, ids)
}

func (e *Executor) WorkflowGetPendingIDs(ctx context.Context, params *riverdriver.WorkflowGetPendingIDsParams) ([]*riverdriver.WorkflowGetPendingIDsRow, error) {
	rows, err := dbsqlc.New().WorkflowGetPendingIDs(schemaTemplateParam(ctx, params.Schema), e.dbtx, pgtype.Text{String: params.WorkflowID, Valid: params.WorkflowID != ""})
	if err != nil {
		return nil, interpretError(err)
	}
	out := make([]*riverdriver.WorkflowGetPendingIDsRow, 0, len(rows))
	for _, row := range rows {
		out = append(out, &riverdriver.WorkflowGetPendingIDsRow{JobID: row.ID, Task: row.Task})
	}
	return out, nil
}

func (e *Executor) WorkflowJobGetByTaskName(ctx context.Context, params *riverdriver.WorkflowJobGetByTaskNameParams) (*rivertype.JobRow, error) {
	id, err := dbsqlc.New().WorkflowJobGetByTaskNameID(schemaTemplateParam(ctx, params.Schema), e.dbtx, &dbsqlc.WorkflowJobGetByTaskNameIDParams{
		TaskName:   pgtype.Text{String: params.TaskName, Valid: params.TaskName != ""},
		WorkflowID: pgtype.Text{String: params.WorkflowID, Valid: params.WorkflowID != ""},
	})
	if err != nil {
		return nil, interpretError(err)
	}
	return e.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: id, Schema: params.Schema})
}

func (e *Executor) WorkflowJobList(ctx context.Context, params *riverdriver.WorkflowJobListParams) ([]*rivertype.JobRow, error) {
	ids, err := dbsqlc.New().WorkflowJobListIDs(schemaTemplateParam(ctx, params.Schema), e.dbtx, pgtype.Text{String: params.WorkflowID, Valid: params.WorkflowID != ""})
	if err != nil {
		return nil, interpretError(err)
	}
	return e.fetchJobsByIDMany(ctx, params.Schema, ids)
}

func (e *Executor) WorkflowListActive(ctx context.Context, params *riverdriver.WorkflowListParams) ([]*riverdriver.WorkflowListItem, error) {
	rows, err := dbsqlc.New().WorkflowListActive(schemaTemplateParam(ctx, params.Schema), e.dbtx, int32(params.Max))
	if err != nil {
		return nil, interpretError(err)
	}
	out := make([]*riverdriver.WorkflowListItem, 0, len(rows))
	for _, row := range rows {
		id := textOrEmpty(row.WorkflowID)
		name := id
		out = append(out, &riverdriver.WorkflowListItem{ID: id, CreatedAt: anyToTime(row.CreatedAt), UpdatedAt: anyToTime(row.UpdatedAt), Name: &name, Metadata: []byte("{}")})
	}
	return out, nil
}

func (e *Executor) WorkflowListAll(ctx context.Context, params *riverdriver.WorkflowListParams) ([]*riverdriver.WorkflowListItem, error) {
	rows, err := dbsqlc.New().WorkflowListAll(schemaTemplateParam(ctx, params.Schema), e.dbtx, int32(params.Max))
	if err != nil {
		return nil, interpretError(err)
	}
	out := make([]*riverdriver.WorkflowListItem, 0, len(rows))
	for _, row := range rows {
		id := textOrEmpty(row.WorkflowID)
		name := id
		out = append(out, &riverdriver.WorkflowListItem{ID: id, CreatedAt: anyToTime(row.CreatedAt), UpdatedAt: anyToTime(row.UpdatedAt), Name: &name, Metadata: []byte("{}")})
	}
	return out, nil
}

func (e *Executor) WorkflowListInactive(ctx context.Context, params *riverdriver.WorkflowListParams) ([]*riverdriver.WorkflowListItem, error) {
	rows, err := dbsqlc.New().WorkflowListInactive(schemaTemplateParam(ctx, params.Schema), e.dbtx, int32(params.Max))
	if err != nil {
		return nil, interpretError(err)
	}
	out := make([]*riverdriver.WorkflowListItem, 0, len(rows))
	for _, row := range rows {
		id := textOrEmpty(row.WorkflowID)
		name := id
		out = append(out, &riverdriver.WorkflowListItem{ID: id, CreatedAt: anyToTime(row.CreatedAt), UpdatedAt: anyToTime(row.UpdatedAt), Name: &name, Metadata: []byte("{}")})
	}
	return out, nil
}

func (e *Executor) WorkflowLoadDepTasksAndIDs(ctx context.Context, params *riverdriver.WorkflowLoadDepTasksAndIDsParams) (map[string]*int64, error) {
	out := make(map[string]*int64, len(params.TaskNames))
	for _, taskName := range params.TaskNames {
		out[taskName] = nil
	}

	if len(params.TaskNames) == 0 {
		return out, nil
	}
	rows, err := dbsqlc.New().WorkflowLoadDepTasksAndIDs(schemaTemplateParam(ctx, params.Schema), e.dbtx, &dbsqlc.WorkflowLoadDepTasksAndIDsParams{WorkflowID: pgtype.Text{String: params.WorkflowID, Valid: params.WorkflowID != ""}, TaskNames: params.TaskNames})
	if err != nil {
		return nil, interpretError(err)
	}
	for _, row := range rows {
		idCopy := row.ID
		out[textOrEmpty(row.Task)] = &idCopy
	}
	return out, nil
}

func parseDeps(raw string) []string {
	raw = strings.TrimSpace(raw)
	if raw == "" || raw == "[]" || raw == "{}" {
		return nil
	}

	if strings.HasPrefix(raw, "[") {
		var deps []string
		if err := json.Unmarshal([]byte(raw), &deps); err == nil {
			return deps
		}
	}

	raw = strings.Trim(raw, "{}")
	if raw == "" {
		return nil
	}
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for i := range parts {
		part := strings.TrimSpace(parts[i])
		part = strings.Trim(part, `"`)
		if part == "" {
			continue
		}
		out = append(out, part)
	}
	return out
}

func textOrEmpty(v pgtype.Text) string {
	if !v.Valid {
		return ""
	}
	return v.String
}

func anyToTime(v any) time.Time {
	switch t := v.(type) {
	case time.Time:
		return t
	case *time.Time:
		if t != nil {
			return *t
		}
	}
	return time.Time{}
}

func anyToString(v any) string {
	switch s := v.(type) {
	case string:
		return s
	case []byte:
		return string(s)
	}
	return ""
}

func (e *Executor) WorkflowLoadJobsWithDeps(ctx context.Context, params *riverdriver.WorkflowLoadJobsWithDepsParams) ([]*riverdriver.WorkflowTaskWithJob, error) {
	jobs, err := e.WorkflowJobList(ctx, &riverdriver.WorkflowJobListParams{Schema: params.Schema, WorkflowID: params.WorkflowID})
	if err != nil {
		return nil, err
	}
	rows, err := dbsqlc.New().WorkflowLoadJobsWithDeps(schemaTemplateParam(ctx, params.Schema), e.dbtx, pgtype.Text{String: params.WorkflowID, Valid: params.WorkflowID != ""})
	if err != nil {
		return nil, interpretError(err)
	}
	jobByID := make(map[int64]*rivertype.JobRow, len(jobs))
	for _, job := range jobs {
		jobByID[job.ID] = job
	}
	out := make([]*riverdriver.WorkflowTaskWithJob, 0, len(rows))
	for _, row := range rows {
		out = append(out, &riverdriver.WorkflowTaskWithJob{
			Task: &riverdriver.WorkflowTask{TaskName: row.Task, Deps: parseDeps(anyToString(row.DepsRaw))},
			Job:  jobByID[row.ID],
		})
	}
	return out, nil
}

func (e *Executor) WorkflowLoadTaskWithDeps(ctx context.Context, params *riverdriver.WorkflowLoadTaskWithDepsParams) (*riverdriver.WorkflowTaskWithJob, error) {
	job, err := e.WorkflowJobGetByTaskName(ctx, &riverdriver.WorkflowJobGetByTaskNameParams{Schema: params.Schema, WorkflowID: params.WorkflowID, TaskName: params.TaskName})
	if err != nil {
		return nil, err
	}
	depsRawAny, err := dbsqlc.New().WorkflowLoadTaskDepsByJobID(schemaTemplateParam(ctx, params.Schema), e.dbtx, job.ID)
	if err != nil {
		return nil, interpretError(err)
	}
	depsRaw, _ := depsRawAny.(string)
	return &riverdriver.WorkflowTaskWithJob{Task: &riverdriver.WorkflowTask{TaskName: params.TaskName, Deps: parseDeps(depsRaw)}, Job: job}, nil
}

func (e *Executor) WorkflowLoadTasksByNames(ctx context.Context, params *riverdriver.WorkflowLoadTasksByNamesParams) ([]*riverdriver.WorkflowTask, error) {
	if len(params.TaskNames) == 0 {
		return nil, nil
	}
	rows, err := dbsqlc.New().WorkflowLoadTasksByNames(schemaTemplateParam(ctx, params.Schema), e.dbtx, &dbsqlc.WorkflowLoadTasksByNamesParams{WorkflowID: pgtype.Text{String: params.WorkflowID, Valid: params.WorkflowID != ""}, TaskNames: params.TaskNames})
	if err != nil {
		return nil, interpretError(err)
	}
	out := make([]*riverdriver.WorkflowTask, 0, len(rows))
	for _, row := range rows {
		out = append(out, &riverdriver.WorkflowTask{TaskName: row.Task, Deps: parseDeps(anyToString(row.DepsRaw))})
	}
	return out, nil
}

func (e *Executor) WorkflowRetry(ctx context.Context, params *riverdriver.WorkflowRetryParams) ([]*rivertype.JobRow, error) {
	rows, err := e.WorkflowLoadJobsWithDeps(ctx, &riverdriver.WorkflowLoadJobsWithDepsParams{Schema: params.Schema, WorkflowID: params.WorkflowID})
	if err != nil {
		return nil, err
	}

	selectedByTask := workflowRetrySelection(rows, params.Mode)
	if len(selectedByTask) == 0 {
		return nil, nil
	}

	ids := make([]int64, 0, len(selectedByTask))
	pendingIDs := make([]int64, 0, len(selectedByTask))
	for _, row := range selectedByTask {
		ids = append(ids, row.Job.ID)
		for _, dep := range row.Task.Deps {
			if _, ok := selectedByTask[dep]; ok {
				pendingIDs = append(pendingIDs, row.Job.ID)
				break
			}
		}
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	sort.Slice(pendingIDs, func(i, j int) bool { return pendingIDs[i] < pendingIDs[j] })

	returnedIDs, err := dbsqlc.New().WorkflowRetryByIDMany(schemaTemplateParam(ctx, params.Schema), e.dbtx, &dbsqlc.WorkflowRetryByIDManyParams{
		IDs:          ids,
		ResetHistory: params.ResetHistory,
		Now:          &params.Now,
	})
	if err != nil {
		return nil, interpretError(err)
	}
	if len(pendingIDs) > 0 {
		if _, err := dbsqlc.New().WorkflowSetPendingByIDMany(schemaTemplateParam(ctx, params.Schema), e.dbtx, pendingIDs); err != nil {
			return nil, interpretError(err)
		}
	}
	return e.fetchJobsByIDMany(ctx, params.Schema, returnedIDs)
}

func workflowRetrySelection(rows []*riverdriver.WorkflowTaskWithJob, mode string) map[string]*riverdriver.WorkflowTaskWithJob {
	byTask := make(map[string]*riverdriver.WorkflowTaskWithJob, len(rows))
	downstream := make(map[string][]string, len(rows))
	failed := make([]string, 0)
	for _, row := range rows {
		if row == nil || row.Task == nil || row.Job == nil || row.Task.TaskName == "" {
			continue
		}
		byTask[row.Task.TaskName] = row
		for _, dep := range row.Task.Deps {
			downstream[dep] = append(downstream[dep], row.Task.TaskName)
		}
		if row.Job.State == rivertype.JobStateDiscarded || (row.Job.State == rivertype.JobStateCancelled && workflowDepsFailed(row.Job.Metadata)) {
			failed = append(failed, row.Task.TaskName)
		}
	}

	selected := make(map[string]*riverdriver.WorkflowTaskWithJob, len(rows))
	switch mode {
	case "", "all":
		for taskName, row := range byTask {
			if row.Job.State == rivertype.JobStateCancelled || row.Job.State == rivertype.JobStateCompleted || row.Job.State == rivertype.JobStateDiscarded {
				selected[taskName] = row
			}
		}
	case "failed_only":
		for _, taskName := range failed {
			selected[taskName] = byTask[taskName]
		}
	case "failed_and_downstream":
		queue := append([]string(nil), failed...)
		for len(queue) > 0 {
			taskName := queue[0]
			queue = queue[1:]
			if _, ok := selected[taskName]; ok {
				continue
			}
			row := byTask[taskName]
			if row == nil {
				continue
			}
			selected[taskName] = row
			queue = append(queue, downstream[taskName]...)
		}
	default:
		for _, taskName := range failed {
			selected[taskName] = byTask[taskName]
		}
	}

	return selected
}

func workflowDepsFailed(metadataBytes []byte) bool {
	if len(metadataBytes) == 0 {
		return false
	}
	metadata := map[string]json.RawMessage{}
	if err := json.Unmarshal(metadataBytes, &metadata); err != nil {
		return false
	}
	_, ok := metadata["workflow_deps_failed_at"]
	return ok
}

func (e *Executor) WorkflowRetryLockAndCheckRunning(ctx context.Context, params *riverdriver.WorkflowRetryLockAndCheckRunningParams) (*riverdriver.WorkflowRetryLockAndCheckRunningResult, error) {
	key := advisoryLockKeyFromString(params.WorkflowID)
	locked, err := e.PGTryAdvisoryXactLock(ctx, key)
	if err != nil {
		return nil, err
	}
	if !locked {
		return &riverdriver.WorkflowRetryLockAndCheckRunningResult{CanRetry: false, WorkflowIsActive: true}, nil
	}
	jobs, err := e.WorkflowJobList(ctx, &riverdriver.WorkflowJobListParams{Schema: params.Schema, WorkflowID: params.WorkflowID})
	if err != nil {
		return nil, err
	}
	workflowIsActive := false
	for _, job := range jobs {
		if job == nil {
			continue
		}
		switch job.State {
		case rivertype.JobStateAvailable, rivertype.JobStatePending, rivertype.JobStateRetryable, rivertype.JobStateRunning, rivertype.JobStateScheduled:
			workflowIsActive = true
		}
		if workflowIsActive {
			break
		}
	}
	return &riverdriver.WorkflowRetryLockAndCheckRunningResult{CanRetry: !workflowIsActive, WorkflowIsActive: workflowIsActive}, nil
}

func advisoryLockKeyFromString(s string) int64 {
	h := hashutil.NewAdvisoryLockHash(0)
	h.Write([]byte(s))
	key := h.Key()
	if key == 0 {
		return 1
	}
	return key
}

func (e *Executor) WorkflowStageJobsByIDMany(ctx context.Context, params *riverdriver.WorkflowStageJobsByIDManyParams) ([]*rivertype.JobRow, error) {
	ids, err := dbsqlc.New().WorkflowStageJobsByIDMany(schemaTemplateParam(ctx, params.Schema), e.dbtx, params.ID)
	if err != nil {
		return nil, interpretError(err)
	}
	return e.fetchJobsByIDMany(ctx, params.Schema, ids)
}
