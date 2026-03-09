package riversqlite

import (
	"context"

	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/rivertype"
)

func (e *Executor) JobDeadLetterDeleteByID(context.Context, *riverdriver.JobDeadLetterDeleteByIDParams) (*rivertype.JobRow, error) {
	return nil, riverdriver.ErrNotImplemented
}
func (e *Executor) JobDeadLetterGetAll(context.Context, *riverdriver.JobDeadLetterGetAllParams) ([]*rivertype.JobRow, error) {
	return nil, riverdriver.ErrNotImplemented
}
func (e *Executor) JobDeadLetterGetByID(context.Context, *riverdriver.JobDeadLetterGetByIDParams) (*rivertype.JobRow, error) {
	return nil, riverdriver.ErrNotImplemented
}
func (e *Executor) JobDeadLetterMoveByID(context.Context, *riverdriver.JobDeadLetterMoveByIDParams) (*rivertype.JobRow, error) {
	return nil, riverdriver.ErrNotImplemented
}
func (e *Executor) JobDeadLetterMoveDiscarded(context.Context, *riverdriver.JobDeadLetterMoveDiscardedParams) ([]*rivertype.JobRow, error) {
	return nil, riverdriver.ErrNotImplemented
}
func (e *Executor) JobDeleteByIDMany(context.Context, *riverdriver.JobDeleteByIDManyParams) ([]*rivertype.JobRow, error) {
	return nil, riverdriver.ErrNotImplemented
}
func (e *Executor) JobGetAvailableForBatch(context.Context, *riverdriver.JobGetAvailableForBatchParams) ([]*rivertype.JobRow, error) {
	return nil, riverdriver.ErrNotImplemented
}
func (e *Executor) JobGetAvailableLimited(context.Context, *riverdriver.JobGetAvailableLimitedParams) ([]*rivertype.JobRow, error) {
	return nil, riverdriver.ErrNotImplemented
}
func (e *Executor) JobGetAvailablePartitionKeys(context.Context, *riverdriver.JobGetAvailablePartitionKeysParams) ([]string, error) {
	return nil, riverdriver.ErrNotImplemented
}
func (e *Executor) PGTryAdvisoryXactLock(context.Context, int64) (bool, error) {
	return false, riverdriver.ErrNotImplemented
}
func (e *Executor) PeriodicJobGetAll(context.Context, *riverdriver.PeriodicJobGetAllParams) ([]*riverdriver.PeriodicJob, error) {
	return nil, riverdriver.ErrNotImplemented
}
func (e *Executor) PeriodicJobGetByID(context.Context, *riverdriver.PeriodicJobGetByIDParams) (*riverdriver.PeriodicJob, error) {
	return nil, riverdriver.ErrNotImplemented
}
func (e *Executor) PeriodicJobInsert(context.Context, *riverdriver.PeriodicJobInsertParams) (*riverdriver.PeriodicJob, error) {
	return nil, riverdriver.ErrNotImplemented
}
func (e *Executor) PeriodicJobKeepAliveAndReap(context.Context, *riverdriver.PeriodicJobKeepAliveAndReapParams) ([]*riverdriver.PeriodicJob, error) {
	return nil, riverdriver.ErrNotImplemented
}
func (e *Executor) PeriodicJobUpsertMany(context.Context, *riverdriver.PeriodicJobUpsertManyParams) ([]*riverdriver.PeriodicJob, error) {
	return nil, riverdriver.ErrNotImplemented
}
func (e *Executor) ProducerDelete(context.Context, *riverdriver.ProducerDeleteParams) error {
	return riverdriver.ErrNotImplemented
}
func (e *Executor) ProducerGetByID(context.Context, *riverdriver.ProducerGetByIDParams) (*riverdriver.Producer, error) {
	return nil, riverdriver.ErrNotImplemented
}
func (e *Executor) ProducerInsertOrUpdate(context.Context, *riverdriver.ProducerInsertOrUpdateParams) (*riverdriver.Producer, error) {
	return nil, riverdriver.ErrNotImplemented
}
func (e *Executor) ProducerKeepAlive(context.Context, *riverdriver.ProducerKeepAliveParams) (*riverdriver.Producer, error) {
	return nil, riverdriver.ErrNotImplemented
}
func (e *Executor) ProducerListByQueue(context.Context, *riverdriver.ProducerListByQueueParams) ([]*riverdriver.ProducerListByQueueResult, error) {
	return nil, riverdriver.ErrNotImplemented
}
func (e *Executor) ProducerUpdate(context.Context, *riverdriver.ProducerUpdateParams) (*riverdriver.Producer, error) {
	return nil, riverdriver.ErrNotImplemented
}
func (e *Executor) QueueGetMetadataForInsert(context.Context, *riverdriver.QueueGetMetadataForInsertParams) ([]*riverdriver.QueueGetMetadataForInsertResult, error) {
	return nil, riverdriver.ErrNotImplemented
}
func (e *Executor) SequenceAppendMany(context.Context, *riverdriver.SequenceAppendManyParams) (int, error) {
	return 0, riverdriver.ErrNotImplemented
}
func (e *Executor) SequenceList(context.Context, *riverdriver.SequenceListParams) ([]*riverdriver.Sequence, error) {
	return nil, riverdriver.ErrNotImplemented
}
func (e *Executor) SequencePromote(context.Context, *riverdriver.SequencePromoteParams) ([]string, error) {
	return nil, riverdriver.ErrNotImplemented
}
func (e *Executor) SequencePromoteFromTable(context.Context, *riverdriver.SequencePromoteFromTableParams) (*riverdriver.SequencePromoteFromTableResult, error) {
	return nil, riverdriver.ErrNotImplemented
}
func (e *Executor) SequenceScanAndPromoteStalled(context.Context, *riverdriver.SequenceScanAndPromoteStalledParams) (*riverdriver.SequenceScanAndPromoteStalledResult, error) {
	return nil, riverdriver.ErrNotImplemented
}
func (e *Executor) WorkflowCancel(context.Context, *riverdriver.WorkflowCancelParams) ([]*rivertype.JobRow, error) {
	return nil, riverdriver.ErrNotImplemented
}
func (e *Executor) WorkflowCancelWithFailedDepsMany(context.Context, *riverdriver.WorkflowCancelWithFailedDepsManyParams) ([]*rivertype.JobRow, error) {
	return nil, riverdriver.ErrNotImplemented
}
func (e *Executor) WorkflowGetPendingIDs(context.Context, *riverdriver.WorkflowGetPendingIDsParams) ([]*riverdriver.WorkflowGetPendingIDsRow, error) {
	return nil, riverdriver.ErrNotImplemented
}
func (e *Executor) WorkflowJobGetByTaskName(context.Context, *riverdriver.WorkflowJobGetByTaskNameParams) (*rivertype.JobRow, error) {
	return nil, riverdriver.ErrNotImplemented
}
func (e *Executor) WorkflowJobList(context.Context, *riverdriver.WorkflowJobListParams) ([]*rivertype.JobRow, error) {
	return nil, riverdriver.ErrNotImplemented
}
func (e *Executor) WorkflowListActive(context.Context, *riverdriver.WorkflowListParams) ([]*riverdriver.WorkflowListItem, error) {
	return nil, riverdriver.ErrNotImplemented
}
func (e *Executor) WorkflowListAll(context.Context, *riverdriver.WorkflowListParams) ([]*riverdriver.WorkflowListItem, error) {
	return nil, riverdriver.ErrNotImplemented
}
func (e *Executor) WorkflowListInactive(context.Context, *riverdriver.WorkflowListParams) ([]*riverdriver.WorkflowListItem, error) {
	return nil, riverdriver.ErrNotImplemented
}
func (e *Executor) WorkflowLoadDepTasksAndIDs(context.Context, *riverdriver.WorkflowLoadDepTasksAndIDsParams) (map[string]*int64, error) {
	return nil, riverdriver.ErrNotImplemented
}
func (e *Executor) WorkflowLoadJobsWithDeps(context.Context, *riverdriver.WorkflowLoadJobsWithDepsParams) ([]*riverdriver.WorkflowTaskWithJob, error) {
	return nil, riverdriver.ErrNotImplemented
}
func (e *Executor) WorkflowLoadTaskWithDeps(context.Context, *riverdriver.WorkflowLoadTaskWithDepsParams) (*riverdriver.WorkflowTaskWithJob, error) {
	return nil, riverdriver.ErrNotImplemented
}
func (e *Executor) WorkflowLoadTasksByNames(context.Context, *riverdriver.WorkflowLoadTasksByNamesParams) ([]*riverdriver.WorkflowTask, error) {
	return nil, riverdriver.ErrNotImplemented
}
func (e *Executor) WorkflowRetry(context.Context, *riverdriver.WorkflowRetryParams) ([]*rivertype.JobRow, error) {
	return nil, riverdriver.ErrNotImplemented
}
func (e *Executor) WorkflowRetryLockAndCheckRunning(context.Context, *riverdriver.WorkflowRetryLockAndCheckRunningParams) (*riverdriver.WorkflowRetryLockAndCheckRunningResult, error) {
	return nil, riverdriver.ErrNotImplemented
}
func (e *Executor) WorkflowStageJobsByIDMany(context.Context, *riverdriver.WorkflowStageJobsByIDManyParams) ([]*rivertype.JobRow, error) {
	return nil, riverdriver.ErrNotImplemented
}
