// Package riverdriver exposes generic constructs to be implemented by specific
// drivers that wrap third party database packages, with the aim being to keep
// the main River interface decoupled from a specific database package so that
// other packages or other major versions of packages can be supported in future
// River versions.
//
// River currently only supports Pgx v5, and the interface here wrap it with
// only the thinnest possible layer. Adding support for alternate packages will
// require the interface to change substantially, and therefore it should not be
// implemented or invoked by user code. Changes to interfaces in this package
// WILL NOT be considered breaking changes for purposes of River's semantic
// versioning.
package riverdriver

import (
	"context"
	"errors"
	"io/fs"
	"time"

	"github.com/riverqueue/river/rivertype"
)

const AllQueuesString = "*"

const MigrationLineMain = "main"

var (
	ErrClosedPool     = errors.New("underlying driver pool is closed")
	ErrNotImplemented = errors.New("driver does not implement this functionality")
)

// Driver provides a database driver for use with river.Client.
//
// Its purpose is to wrap the interface of a third party database package, with
// the aim being to keep the main River interface decoupled from a specific
// database package so that other packages or major versions of packages can be
// supported in future River versions.
//
// River currently only supports Pgx v5, and this interface wraps it with only
// the thinnest possible layer. Adding support for alternate packages will
// require it to change substantially, and therefore it should not be
// implemented or invoked by user code. Changes to this interface WILL NOT be
// considered breaking changes for purposes of River's semantic versioning.
//
// API is not stable. DO NOT IMPLEMENT.
type Driver[TTx any] interface {
	// ArgPlaceholder is the placeholder character used in query positional
	// arguments, so "$" for "$1", "$2", "$3", etc. This is a "$" for Postgres
	// and "?" for SQLite.
	//
	// API is not stable. DO NOT USE.
	ArgPlaceholder() string

	// DatabaseName is the name of the database that the driver targets like
	// "postgres" or "sqlite". This is used for purposes like a cache key prefix
	// in riverdbtest so that multiple drivers may share schemas as long as they
	// target the same database.
	//
	// API is not stable. DO NOT USE.
	DatabaseName() string

	// GetExecutor gets an executor for the driver.
	//
	// API is not stable. DO NOT USE.
	GetExecutor() Executor

	// GetListener gets a listener for purposes of receiving notifications.
	//
	// API is not stable. DO NOT USE.
	GetListener(params *GetListenenerParams) Listener

	// GetMigrationDefaultLines gets default migration lines that should be
	// applied when using this driver. This is mainly used by riverdbtest to
	// figure out what migration lines should be available by default for new
	// test schemas.
	//
	// API is not stable. DO NOT USE.
	GetMigrationDefaultLines() []string

	// GetMigrationFS gets a filesystem containing migrations for the driver.
	//
	// Each set of migration files is expected to exist within the filesystem as
	// `migration/<line>/`. For example:
	//
	//     migration/main/001_create_river_migration.up.sql
	//
	// API is not stable. DO NOT USE.
	GetMigrationFS(line string) fs.FS

	// GetMigrationLines gets supported migration lines from the driver. Most
	// drivers will only support a single line: MigrationLineMain.
	//
	// API is not stable. DO NOT USE.
	GetMigrationLines() []string

	// GetMigrationTruncateTables gets the tables that should be truncated
	// before or after tests for a specific migration line returned by this
	// driver. Tables to truncate doesn't need to consider intermediary states,
	// and should return tables for the latest migration version.
	//
	// API is not stable. DO NOT USE.
	GetMigrationTruncateTables(line string, version int) []string

	// PoolIsSet returns true if the driver is configured with a database pool.
	//
	// API is not stable. DO NOT USE.
	PoolIsSet() bool

	// PoolSet sets a database pool into a driver will a nil pool. This is meant
	// only for use in testing, and only in specific circumstances where it's
	// needed. The pool in a driver should generally be treated as immutable
	// because it's inherited by driver executors, and changing if when active
	// executors exist will cause problems.
	//
	// Most drivers don't implement this function and return ErrNotImplemented.
	//
	// Drivers should only set a pool if the previous pool was nil (to help root
	// out bugs where something unexpected is happening), and panic in case a
	// pool is set to a driver twice.
	//
	// API is not stable. DO NOT USE.
	PoolSet(dbPool any) error

	// SQLFragmentColumnIn generates an SQL fragment to be included as a
	// predicate in a `WHERE` query for the existence of a set of values in a
	// column like `id IN (...)`. The actual implementation depends on support
	// for specific data types. Postgres uses arrays while SQLite uses a JSON
	// fragment with `json_each`.
	//
	// API is not stable. DO NOT USE.
	SQLFragmentColumnIn(column string, values any) (string, any, error)

	// SupportsListener gets whether this driver supports a listener. Drivers
	// that don't support a listener support poll only mode only.
	//
	// API is not stable. DO NOT USE.
	SupportsListener() bool

	// SupportsListenNotify indicates whether the underlying database supports
	// listen/notify. This differs from SupportsListener in that even if a
	// driver doesn't a support a listener but the database supports the
	// underlying listen/notify mechanism, it will still broadcast in case there
	// are other clients/drivers on the database that do support a listener. If
	// listen/notify can't be supported at all, no broadcast attempt is made.
	//
	// API is not stable. DO NOT USE.
	SupportsListenNotify() bool

	// TimePrecision returns the maximum time resolution supported by the
	// database. This is used in test assertions when checking round trips on
	// timestamps.
	//
	// API is not stable. DO NOT USE.
	TimePrecision() time.Duration

	// UnwrapExecutor gets an executor from a driver transaction.
	//
	// API is not stable. DO NOT USE.
	UnwrapExecutor(tx TTx) ExecutorTx

	// UnwrapTx gets a driver transaction from an executor. This is currently
	// only needed for test transaction helpers.
	//
	// API is not stable. DO NOT USE.
	UnwrapTx(execTx ExecutorTx) TTx
}

// Executor provides River operations against a database. It may be a database
// pool or transaction.
//
// API is not stable. DO NOT IMPLEMENT.
type Executor interface {
	// Begin begins a new subtransaction. ErrSubTxNotSupported may be returned
	// if the executor is a transaction and the driver doesn't support
	// subtransactions (like riverdriver/riverdatabasesql for database/sql).
	Begin(ctx context.Context) (ExecutorTx, error)

	// ColumnExists checks whether a column for a particular table exists for
	// the schema in the current search schema.
	ColumnExists(ctx context.Context, params *ColumnExistsParams) (bool, error)

	// Exec executes raw SQL. Used for migrations.
	Exec(ctx context.Context, sql string, args ...any) error

	// IndexDropIfExists drops a database index if exists. This abstraction is a
	// little leaky right now because Postgres runs this `CONCURRENTLY` and
	// that's not possible in SQLite.
	//
	// API is not stable. DO NOT USE.
	IndexDropIfExists(ctx context.Context, params *IndexDropIfExistsParams) error
	IndexExists(ctx context.Context, params *IndexExistsParams) (bool, error)
	IndexesExist(ctx context.Context, params *IndexesExistParams) (map[string]bool, error)

	// IndexReindex reindexes a database index. This abstraction is a little
	// leaky right now because Postgres runs this `CONCURRENTLY` and that's not
	// possible in SQLite.
	//
	// API is not stable. DO NOT USE.
	IndexReindex(ctx context.Context, params *IndexReindexParams) error

	JobCancel(ctx context.Context, params *JobCancelParams) (*rivertype.JobRow, error)
	JobCountByAllStates(ctx context.Context, params *JobCountByAllStatesParams) (map[rivertype.JobState]int, error)
	JobCountByQueueAndState(ctx context.Context, params *JobCountByQueueAndStateParams) ([]*JobCountByQueueAndStateResult, error)
	JobCountByState(ctx context.Context, params *JobCountByStateParams) (int, error)
	JobDelete(ctx context.Context, params *JobDeleteParams) (*rivertype.JobRow, error)
	JobDeadLetterDeleteByID(ctx context.Context, params *JobDeadLetterDeleteByIDParams) (*rivertype.JobRow, error)
	JobDeadLetterGetAll(ctx context.Context, params *JobDeadLetterGetAllParams) ([]*rivertype.JobRow, error)
	JobDeadLetterGetByID(ctx context.Context, params *JobDeadLetterGetByIDParams) (*rivertype.JobRow, error)
	JobDeadLetterMoveByID(ctx context.Context, params *JobDeadLetterMoveByIDParams) (*rivertype.JobRow, error)
	JobDeadLetterMoveDiscarded(ctx context.Context, params *JobDeadLetterMoveDiscardedParams) ([]*rivertype.JobRow, error)
	JobDeleteBefore(ctx context.Context, params *JobDeleteBeforeParams) (int, error)
	JobDeleteByIDMany(ctx context.Context, params *JobDeleteByIDManyParams) ([]*rivertype.JobRow, error)
	JobDeleteMany(ctx context.Context, params *JobDeleteManyParams) ([]*rivertype.JobRow, error)
	JobGetAvailable(ctx context.Context, params *JobGetAvailableParams) ([]*rivertype.JobRow, error)
	JobGetAvailableForBatch(ctx context.Context, params *JobGetAvailableForBatchParams) ([]*rivertype.JobRow, error)
	JobGetAvailableLimited(ctx context.Context, params *JobGetAvailableLimitedParams) ([]*rivertype.JobRow, error)
	JobGetAvailablePartitionKeys(ctx context.Context, params *JobGetAvailablePartitionKeysParams) ([]string, error)
	JobGetByID(ctx context.Context, params *JobGetByIDParams) (*rivertype.JobRow, error)
	JobGetByIDMany(ctx context.Context, params *JobGetByIDManyParams) ([]*rivertype.JobRow, error)
	JobGetByKindMany(ctx context.Context, params *JobGetByKindManyParams) ([]*rivertype.JobRow, error)
	JobGetStuck(ctx context.Context, params *JobGetStuckParams) ([]*rivertype.JobRow, error)
	JobInsertFastMany(ctx context.Context, params *JobInsertFastManyParams) ([]*JobInsertFastResult, error)
	JobInsertFastManyNoReturning(ctx context.Context, params *JobInsertFastManyParams) (int, error)
	JobInsertFull(ctx context.Context, params *JobInsertFullParams) (*rivertype.JobRow, error)
	JobInsertFullMany(ctx context.Context, jobs *JobInsertFullManyParams) ([]*rivertype.JobRow, error)
	JobKindList(ctx context.Context, params *JobKindListParams) ([]string, error)
	JobList(ctx context.Context, params *JobListParams) ([]*rivertype.JobRow, error)
	JobRescueMany(ctx context.Context, params *JobRescueManyParams) (*struct{}, error)
	JobRetry(ctx context.Context, params *JobRetryParams) (*rivertype.JobRow, error)
	JobSchedule(ctx context.Context, params *JobScheduleParams) ([]*JobScheduleResult, error)
	JobSetStateIfRunningMany(ctx context.Context, params *JobSetStateIfRunningManyParams) ([]*rivertype.JobRow, error)
	JobUpdate(ctx context.Context, params *JobUpdateParams) (*rivertype.JobRow, error)
	JobUpdateFull(ctx context.Context, params *JobUpdateFullParams) (*rivertype.JobRow, error)
	LeaderAttemptElect(ctx context.Context, params *LeaderElectParams) (bool, error)
	LeaderAttemptReelect(ctx context.Context, params *LeaderElectParams) (bool, error)
	LeaderDeleteExpired(ctx context.Context, params *LeaderDeleteExpiredParams) (int, error)
	LeaderGetElectedLeader(ctx context.Context, params *LeaderGetElectedLeaderParams) (*Leader, error)
	LeaderInsert(ctx context.Context, params *LeaderInsertParams) (*Leader, error)
	LeaderResign(ctx context.Context, params *LeaderResignParams) (bool, error)

	// MigrationDeleteAssumingMainMany deletes many migrations assuming
	// everything is on the main line. This is suitable for use in databases on
	// a version before the `line` column exists.
	MigrationDeleteAssumingMainMany(ctx context.Context, params *MigrationDeleteAssumingMainManyParams) ([]*Migration, error)

	// MigrationDeleteByLineAndVersionMany deletes many migration versions on a
	// particular line.
	MigrationDeleteByLineAndVersionMany(ctx context.Context, params *MigrationDeleteByLineAndVersionManyParams) ([]*Migration, error)

	// MigrationGetAllAssumingMain gets all migrations assuming everything is on
	// the main line. This is suitable for use in databases on a version before
	// the `line` column exists.
	MigrationGetAllAssumingMain(ctx context.Context, params *MigrationGetAllAssumingMainParams) ([]*Migration, error)

	// MigrationGetByLine gets all currently applied migrations.
	MigrationGetByLine(ctx context.Context, params *MigrationGetByLineParams) ([]*Migration, error)

	// MigrationInsertMany inserts many migration versions.
	MigrationInsertMany(ctx context.Context, params *MigrationInsertManyParams) ([]*Migration, error)

	// MigrationInsertManyAssumingMain inserts many migrations, assuming they're
	// on the main line. This operation is necessary for compatibility before
	// the `line` column was added to the migrations table.
	MigrationInsertManyAssumingMain(ctx context.Context, params *MigrationInsertManyAssumingMainParams) ([]*Migration, error)

	NotifyMany(ctx context.Context, params *NotifyManyParams) error
	PGAdvisoryXactLock(ctx context.Context, key int64) (*struct{}, error)
	PGTryAdvisoryXactLock(ctx context.Context, key int64) (bool, error)

	PeriodicJobGetAll(ctx context.Context, params *PeriodicJobGetAllParams) ([]*PeriodicJob, error)
	PeriodicJobGetByID(ctx context.Context, params *PeriodicJobGetByIDParams) (*PeriodicJob, error)
	PeriodicJobInsert(ctx context.Context, params *PeriodicJobInsertParams) (*PeriodicJob, error)
	PeriodicJobKeepAliveAndReap(ctx context.Context, params *PeriodicJobKeepAliveAndReapParams) ([]*PeriodicJob, error)
	PeriodicJobUpsertMany(ctx context.Context, params *PeriodicJobUpsertManyParams) ([]*PeriodicJob, error)

	ProducerDelete(ctx context.Context, params *ProducerDeleteParams) error
	ProducerGetByID(ctx context.Context, params *ProducerGetByIDParams) (*Producer, error)
	ProducerInsertOrUpdate(ctx context.Context, params *ProducerInsertOrUpdateParams) (*Producer, error)
	ProducerKeepAlive(ctx context.Context, params *ProducerKeepAliveParams) (*Producer, error)
	ProducerListByQueue(ctx context.Context, params *ProducerListByQueueParams) ([]*ProducerListByQueueResult, error)
	ProducerUpdate(ctx context.Context, params *ProducerUpdateParams) (*Producer, error)

	QueueGetMetadataForInsert(ctx context.Context, params *QueueGetMetadataForInsertParams) ([]*QueueGetMetadataForInsertResult, error)

	SequenceAppendMany(ctx context.Context, params *SequenceAppendManyParams) (int, error)
	SequenceList(ctx context.Context, params *SequenceListParams) ([]*Sequence, error)
	SequencePromote(ctx context.Context, params *SequencePromoteParams) ([]string, error)
	SequencePromoteFromTable(ctx context.Context, params *SequencePromoteFromTableParams) (*SequencePromoteFromTableResult, error)
	SequenceScanAndPromoteStalled(ctx context.Context, params *SequenceScanAndPromoteStalledParams) (*SequenceScanAndPromoteStalledResult, error)

	WorkflowCancel(ctx context.Context, params *WorkflowCancelParams) ([]*rivertype.JobRow, error)
	WorkflowCancelWithFailedDepsMany(ctx context.Context, params *WorkflowCancelWithFailedDepsManyParams) ([]*rivertype.JobRow, error)
	WorkflowGetPendingIDs(ctx context.Context, params *WorkflowGetPendingIDsParams) ([]*WorkflowGetPendingIDsRow, error)
	WorkflowJobGetByTaskName(ctx context.Context, params *WorkflowJobGetByTaskNameParams) (*rivertype.JobRow, error)
	WorkflowJobList(ctx context.Context, params *WorkflowJobListParams) ([]*rivertype.JobRow, error)
	WorkflowListActive(ctx context.Context, params *WorkflowListParams) ([]*WorkflowListItem, error)
	WorkflowListAll(ctx context.Context, params *WorkflowListParams) ([]*WorkflowListItem, error)
	WorkflowListInactive(ctx context.Context, params *WorkflowListParams) ([]*WorkflowListItem, error)
	WorkflowLoadDepTasksAndIDs(ctx context.Context, params *WorkflowLoadDepTasksAndIDsParams) (map[string]*int64, error)
	WorkflowLoadJobsWithDeps(ctx context.Context, params *WorkflowLoadJobsWithDepsParams) ([]*WorkflowTaskWithJob, error)
	WorkflowLoadTaskWithDeps(ctx context.Context, params *WorkflowLoadTaskWithDepsParams) (*WorkflowTaskWithJob, error)
	WorkflowLoadTasksByNames(ctx context.Context, params *WorkflowLoadTasksByNamesParams) ([]*WorkflowTask, error)
	WorkflowRetry(ctx context.Context, params *WorkflowRetryParams) ([]*rivertype.JobRow, error)
	WorkflowRetryLockAndCheckRunning(ctx context.Context, params *WorkflowRetryLockAndCheckRunningParams) (*WorkflowRetryLockAndCheckRunningResult, error)
	WorkflowStageJobsByIDMany(ctx context.Context, params *WorkflowStageJobsByIDManyParams) ([]*rivertype.JobRow, error)

	QueueCreateOrSetUpdatedAt(ctx context.Context, params *QueueCreateOrSetUpdatedAtParams) (*rivertype.Queue, error)
	QueueDeleteExpired(ctx context.Context, params *QueueDeleteExpiredParams) ([]string, error)
	QueueGet(ctx context.Context, params *QueueGetParams) (*rivertype.Queue, error)
	QueueList(ctx context.Context, params *QueueListParams) ([]*rivertype.Queue, error)
	QueueNameList(ctx context.Context, params *QueueNameListParams) ([]string, error)
	QueuePause(ctx context.Context, params *QueuePauseParams) error
	QueueResume(ctx context.Context, params *QueueResumeParams) error
	QueueUpdate(ctx context.Context, params *QueueUpdateParams) (*rivertype.Queue, error)
	QueryRow(ctx context.Context, sql string, args ...any) Row

	SchemaCreate(ctx context.Context, params *SchemaCreateParams) error
	SchemaDrop(ctx context.Context, params *SchemaDropParams) error
	SchemaGetExpired(ctx context.Context, params *SchemaGetExpiredParams) ([]string, error)

	// TableExists checks whether a table exists for the schema in the current
	// search schema.
	TableExists(ctx context.Context, params *TableExistsParams) (bool, error)
	TableTruncate(ctx context.Context, params *TableTruncateParams) error
}

// ExecutorTx is an executor which is a transaction. In addition to standard
// Executor operations, it may be committed or rolled back.
//
// API is not stable. DO NOT IMPLEMENT.
type ExecutorTx interface {
	Executor

	// Commit commits the transaction.
	//
	// API is not stable. DO NOT USE.
	Commit(ctx context.Context) error

	// Rollback rolls back the transaction.
	//
	// API is not stable. DO NOT USE.
	Rollback(ctx context.Context) error
}

type GetListenenerParams struct {
	Schema string
}

// Listener listens for notifications. In Postgres, this is a database
// connection where `LISTEN` has been run.
//
// API is not stable. DO NOT IMPLEMENT.
type Listener interface {
	Close(ctx context.Context) error
	Connect(ctx context.Context) error
	Listen(ctx context.Context, topic string) error
	Ping(ctx context.Context) error
	Schema() string
	SetAfterConnectExec(sql string) // should only ever be used in testing
	Unlisten(ctx context.Context, topic string) error
	WaitForNotification(ctx context.Context) (*Notification, error)
}

type Notification struct {
	Payload string
	Topic   string
}

type ColumnExistsParams struct {
	Column string
	Schema string
	Table  string
}

type IndexDropIfExistsParams struct {
	Index  string
	Schema string
}

type IndexExistsParams struct {
	Index  string
	Schema string
}

type IndexesExistParams struct {
	IndexNames []string
	Schema     string
}

type JobCancelParams struct {
	ID                int64
	CancelAttemptedAt time.Time
	ControlTopic      string
	Now               *time.Time
	Schema            string
}

type JobCountByAllStatesParams struct {
	Schema string
}

type JobCountByQueueAndStateParams struct {
	QueueNames []string
	Schema     string
}

type JobCountByQueueAndStateResult struct {
	CountAvailable int64
	CountRunning   int64
	Queue          string
}

type JobCountByStateParams struct {
	Schema string
	State  rivertype.JobState
}

type JobDeleteParams struct {
	ID     int64
	Schema string
}

type JobDeadLetterDeleteByIDParams struct {
	ID     int64
	Schema string
}

type JobDeadLetterGetAllParams struct {
	Max    int
	Schema string
}

type JobDeadLetterGetByIDParams struct {
	ID     int64
	Schema string
}

type JobDeadLetterMoveByIDParams struct {
	ID     int64
	Schema string
}

type JobDeadLetterMoveDiscardedParams struct {
	DiscardedFinalizedAtHorizon time.Time
	Max                         int
	Schema                      string
}

type JobDeleteBeforeParams struct {
	CancelledDoDelete           bool
	CancelledFinalizedAtHorizon time.Time
	CompletedDoDelete           bool
	CompletedFinalizedAtHorizon time.Time
	DiscardedDoDelete           bool
	DiscardedFinalizedAtHorizon time.Time
	Max                         int
	QueuesExcluded              []string
	QueuesIncluded              []string
	Schema                      string
}

type JobDeleteManyParams struct {
	Max           int32
	NamedArgs     map[string]any
	OrderByClause string
	Schema        string
	WhereClause   string
}

type JobDeleteByIDManyParams struct {
	ID     []int64
	Schema string
}

type JobGetAvailableParams struct {
	AvailablePartitionKeys []string
	ClientID               string
	GlobalLimit            int32
	LocalLimit             int32
	MaxAttemptedBy         int
	MaxToLock              int
	Now                    *time.Time
	PartitionByArgs        []string
	PartitionByKind        bool
	ProducerID             int64
	Queue                  string
	Schema                 string
}

type JobGetAvailableForBatchParams struct {
	AttemptedBy      string
	BatchID          string
	BatchKey         string
	BatchLeaderJobID int64
	ClientID         string
	Kind             string
	Max              int32
	MaxAttemptedBy   int
	MaxToLock        int
	Now              *time.Time
	ProducerID       int64
	Queue            string
	Schema           string
}

type JobGetAvailableLimitedParams struct {
	JobGetAvailableParams *JobGetAvailableParams

	AvailablePartitionKeys                []string
	ClientID                              string
	CurrentProducerPartitionKeys          []string
	CurrentProducerPartitionRunningCounts []int32
	GlobalLimit                           int32
	LocalLimit                            int32
	MaxAttemptedBy                        int
	MaxToLock                             int
	Now                                   *time.Time
	PartitionByArgs                       []string
	PartitionByKind                       bool
	ProducerID                            int64
	Queue                                 string
	Schema                                string
}

type JobGetAvailablePartitionKeysParams struct {
	PartitionByArgs []string
	PartitionByKind bool
	Queue           string
	Schema          string
}

type JobGetByIDParams struct {
	ID     int64
	Schema string
}

type JobGetByIDManyParams struct {
	ID     []int64
	Schema string
}

type JobGetByKindManyParams struct {
	Kind   []string
	Schema string
}

type JobGetStuckParams struct {
	Max          int
	Schema       string
	StuckHorizon time.Time
}

type JobInsertFastParams struct {
	ID *int64
	// Args contains the raw underlying job arguments struct. It has already been
	// encoded into EncodedArgs, but the original is kept here for to leverage its
	// struct tags and interfaces, such as for use in unique key generation.
	Args         rivertype.JobArgs
	CreatedAt    *time.Time
	EncodedArgs  []byte
	Kind         string
	MaxAttempts  int
	Metadata     []byte
	Priority     int
	Queue        string
	ScheduledAt  *time.Time
	State        rivertype.JobState
	Tags         []string
	UniqueKey    []byte
	UniqueStates byte
}

type JobInsertFastManyParams struct {
	Jobs   []*JobInsertFastParams
	Schema string
}

type JobInsertFastResult struct {
	Job                      *rivertype.JobRow
	UniqueSkippedAsDuplicate bool
}

type JobInsertFullParams struct {
	Attempt      int
	AttemptedAt  *time.Time
	AttemptedBy  []string
	CreatedAt    *time.Time
	EncodedArgs  []byte
	Errors       [][]byte
	FinalizedAt  *time.Time
	Kind         string
	MaxAttempts  int
	Metadata     []byte
	Priority     int
	Queue        string
	ScheduledAt  *time.Time
	Schema       string
	State        rivertype.JobState
	Tags         []string
	UniqueKey    []byte
	UniqueStates byte
}

type JobInsertFullManyParams struct {
	Jobs   []*JobInsertFullParams
	Schema string
}

type JobKindListParams struct {
	After   string
	Exclude []string
	Match   string
	Max     int
	Schema  string
}

type JobListParams struct {
	Max           int32
	NamedArgs     map[string]any
	OrderByClause string
	Schema        string
	WhereClause   string
}

type JobRescueManyParams struct {
	ID          []int64
	Error       [][]byte
	FinalizedAt []*time.Time
	ScheduledAt []time.Time
	Schema      string
	State       []string
}

type JobRetryParams struct {
	ID     int64
	Now    *time.Time
	Schema string
}

type JobScheduleParams struct {
	Max    int
	Now    *time.Time
	Schema string
}

type JobScheduleResult struct {
	Job               rivertype.JobRow
	ConflictDiscarded bool
}

// JobSetStateIfRunningParams are parameters to update the state of a currently
// running job. Use one of the constructors below to ensure a correct
// combination of parameters.
type JobSetStateIfRunningParams struct {
	ID              int64
	Attempt         *int
	ErrData         []byte
	FinalizedAt     *time.Time
	MetadataDoMerge bool
	MetadataUpdates []byte
	ScheduledAt     *time.Time
	Schema          string // added by completer
	Snoozed         bool
	State           rivertype.JobState
}

func JobSetStateCancelled(id int64, finalizedAt time.Time, errData []byte, metadataUpdates []byte) *JobSetStateIfRunningParams {
	return &JobSetStateIfRunningParams{
		ID:              id,
		ErrData:         errData,
		MetadataDoMerge: len(metadataUpdates) > 0,
		MetadataUpdates: metadataUpdates,
		FinalizedAt:     &finalizedAt,
		State:           rivertype.JobStateCancelled,
	}
}

func JobSetStateCompleted(id int64, finalizedAt time.Time, metadataUpdates []byte) *JobSetStateIfRunningParams {
	return &JobSetStateIfRunningParams{
		FinalizedAt:     &finalizedAt,
		ID:              id,
		MetadataDoMerge: len(metadataUpdates) > 0,
		MetadataUpdates: metadataUpdates,
		State:           rivertype.JobStateCompleted,
	}
}

func JobSetStateDiscarded(id int64, finalizedAt time.Time, errData []byte, metadataUpdates []byte) *JobSetStateIfRunningParams {
	return &JobSetStateIfRunningParams{
		ID:              id,
		ErrData:         errData,
		MetadataDoMerge: len(metadataUpdates) > 0,
		MetadataUpdates: metadataUpdates,
		FinalizedAt:     &finalizedAt,
		State:           rivertype.JobStateDiscarded,
	}
}

func JobSetStateErrorAvailable(id int64, scheduledAt time.Time, errData []byte, metadataUpdates []byte) *JobSetStateIfRunningParams {
	return &JobSetStateIfRunningParams{
		ID:              id,
		ErrData:         errData,
		MetadataDoMerge: len(metadataUpdates) > 0,
		MetadataUpdates: metadataUpdates,
		ScheduledAt:     &scheduledAt,
		State:           rivertype.JobStateAvailable,
	}
}

func JobSetStateErrorRetryable(id int64, scheduledAt time.Time, errData []byte, metadataUpdates []byte) *JobSetStateIfRunningParams {
	return &JobSetStateIfRunningParams{
		ID:              id,
		ErrData:         errData,
		MetadataDoMerge: len(metadataUpdates) > 0,
		MetadataUpdates: metadataUpdates,
		ScheduledAt:     &scheduledAt,
		State:           rivertype.JobStateRetryable,
	}
}

func JobSetStateSnoozed(id int64, scheduledAt time.Time, attempt int, metadataUpdates []byte) *JobSetStateIfRunningParams {
	return &JobSetStateIfRunningParams{
		Attempt:         &attempt,
		ID:              id,
		MetadataDoMerge: len(metadataUpdates) > 0,
		MetadataUpdates: metadataUpdates,
		ScheduledAt:     &scheduledAt,
		Snoozed:         true,
		State:           rivertype.JobStateScheduled,
	}
}

func JobSetStateSnoozedAvailable(id int64, scheduledAt time.Time, attempt int, metadataUpdates []byte) *JobSetStateIfRunningParams {
	return &JobSetStateIfRunningParams{
		Attempt:         &attempt,
		ID:              id,
		MetadataDoMerge: len(metadataUpdates) > 0,
		MetadataUpdates: metadataUpdates,
		ScheduledAt:     &scheduledAt,
		Snoozed:         true,
		State:           rivertype.JobStateAvailable,
	}
}

// JobSetStateIfRunningManyParams are parameters to update the state of
// currently running jobs. Use one of the constructors below to ensure a correct
// combination of parameters.
type JobSetStateIfRunningManyParams struct {
	ID              []int64
	Attempt         []*int
	ErrData         [][]byte
	FinalizedAt     []*time.Time
	MetadataDoMerge []bool
	MetadataUpdates [][]byte
	Now             *time.Time
	ScheduledAt     []*time.Time
	Schema          string
	State           []rivertype.JobState
}

type JobUpdateParams struct {
	ID              int64
	MetadataDoMerge bool
	Metadata        []byte
	Schema          string
}

type JobUpdateFullParams struct {
	ID                  int64
	AttemptDoUpdate     bool
	Attempt             int
	AttemptedAtDoUpdate bool
	AttemptedAt         *time.Time
	AttemptedByDoUpdate bool
	AttemptedBy         []string
	ErrorsDoUpdate      bool
	Errors              [][]byte
	FinalizedAtDoUpdate bool
	FinalizedAt         *time.Time
	MaxAttemptsDoUpdate bool
	MaxAttempts         int
	MetadataDoUpdate    bool
	Metadata            []byte
	Schema              string
	StateDoUpdate       bool
	State               rivertype.JobState
	// Deprecated and will be removed when advisory lock unique path is removed.
	UniqueKeyDoUpdate bool
	// Deprecated and will be removed when advisory lock unique path is removed.
	UniqueKey []byte
}

// Leader represents a River leader.
//
// API is not stable. DO NOT USE.
type Leader struct {
	ElectedAt time.Time
	ExpiresAt time.Time
	LeaderID  string
}

type LeaderDeleteExpiredParams struct {
	Now    *time.Time
	Schema string
}

type LeaderGetElectedLeaderParams struct {
	Schema string
}

type LeaderInsertParams struct {
	ElectedAt *time.Time
	ExpiresAt *time.Time
	LeaderID  string
	Now       *time.Time
	Schema    string
	TTL       time.Duration
}

type LeaderElectParams struct {
	LeaderID string
	Now      *time.Time
	Schema   string
	TTL      time.Duration
}

type LeaderResignParams struct {
	LeaderID        string
	LeadershipTopic string
	Schema          string
}

// Migration represents a River migration.
//
// API is not stable. DO NOT USE.
type Migration struct {
	// CreatedAt is when the migration was initially created.
	//
	// API is not stable. DO NOT USE.
	CreatedAt time.Time

	// Line is the migration line that the migration belongs to.
	//
	// API is not stable. DO NOT USE.
	Line string

	// Version is the version of the migration.
	//
	// API is not stable. DO NOT USE.
	Version int
}

type MigrationDeleteAssumingMainManyParams struct {
	Schema   string
	Versions []int
}

type MigrationDeleteByLineAndVersionManyParams struct {
	Line     string
	Schema   string
	Versions []int
}

type MigrationGetAllAssumingMainParams struct {
	Schema string
}

type MigrationGetByLineParams struct {
	Line   string
	Schema string
}

type MigrationInsertManyParams struct {
	Line     string
	Schema   string
	Versions []int
}

type MigrationInsertManyAssumingMainParams struct {
	Schema   string
	Versions []int
}

// NotifyManyParams are parameters to issue many pubsub notifications all at
// once for a single topic.
type NotifyManyParams struct {
	Payload []string
	Topic   string
	Schema  string
}

type ProducerKeepAliveParams struct {
	ID                    int64
	QueueName             string
	Schema                string
	StaleUpdatedAtHorizon time.Time
}

type PeriodicJob struct {
	ID        int64
	IDString  string
	CreatedAt time.Time
	UpdatedAt time.Time
	Name      string
	Queue     string
	NextRunAt time.Time
	Metadata  []byte
}

type PeriodicJobGetAllParams struct {
	Max                   int
	Schema                string
	StaleUpdatedAtHorizon time.Time
}

type PeriodicJobGetByIDParams struct {
	ID       int64
	IDString string
	Schema   string
}

type PeriodicJobInsertParams struct {
	ID        string
	Metadata  []byte
	Name      string
	NextRunAt time.Time
	Queue     string
	Schema    string
	UpdatedAt *time.Time
}

type PeriodicJobKeepAliveAndReapParams struct {
	ID                    int64
	IDList                []string
	Now                   *time.Time
	Schema                string
	StaleUpdatedAtHorizon time.Time
}

type PeriodicJobUpsertManyParams struct {
	Jobs         []*PeriodicJobUpsertParams
	PeriodicJobs []*PeriodicJobInsertParams
	Schema       string
}

type PeriodicJobUpsertParams struct {
	ID        string
	NextRunAt time.Time
	UpdatedAt time.Time
}

type Producer struct {
	ID         int64
	ClientID   string
	MaxWorkers int32
	PausedAt   *time.Time
	CreatedAt  time.Time
	UpdatedAt  time.Time
	QueueName  string
	Metadata   []byte
}

type ProducerDeleteParams struct {
	ID     int64
	Schema string
}

type ProducerGetByIDParams struct {
	ID     int64
	Schema string
}

type ProducerInsertOrUpdateParams struct {
	ID         int64
	ClientID   string
	CreatedAt  *time.Time
	MaxWorkers int32
	Metadata   []byte
	Now        *time.Time
	PausedAt   *time.Time
	QueueName  string
	Schema     string
	UpdatedAt  *time.Time
}

type ProducerListByQueueParams struct {
	QueueName string
	Schema    string
}

type ProducerListByQueueResult struct {
	Producer  *Producer
	Running   int32
	ID        int64
	QueueName string
	Metadata  []byte
	UpdatedAt time.Time
}

type ProducerUpdateParams struct {
	ID                 int64
	MaxWorkers         int32
	MaxWorkersDoUpdate bool
	Metadata           []byte
	MetadataDoUpdate   bool
	PausedAt           *time.Time
	PausedAtDoUpdate   bool
	Schema             string
	UpdatedAt          *time.Time
}

type QueueGetMetadataForInsertParams struct {
	Names      []string
	QueueNames []string
	Schema     string
}

type QueueGetMetadataForInsertResult struct {
	Concurrency []byte
	Name        string
	Metadata    []byte
}

type Sequence struct {
	ID          int64
	CreatedAt   time.Time
	UpdatedAt   time.Time
	Queue       string
	Key         string
	LatestJobID int64
}

type SequenceAppendManyParams struct {
	JobIDs  []int64
	Keys    []string
	SeqKeys []string
	Queue   string
	Schema  string
}

type SequenceListParams struct {
	Max      int
	MaxCount int
	Queue    string
	Schema   string
}

type SequencePromoteParams struct {
	GracePeriod time.Duration
	Keys        []string
	Max         int
	Now         *time.Time
	Queue       string
	Schema      string
}

type SequencePromoteFromTableParams struct {
	GracePeriod time.Duration
	Max         int
	Now         *time.Time
	Schema      string
}

type SequencePromoteFromTableResult struct {
	Continue       bool
	NumDeleted     int
	PromotedKeys   []string
	PromotedQueues []string
}

type SequenceScanAndPromoteStalledParams struct {
	GracePeriod     time.Duration
	LastSequenceKey string
	Max             int
	Now             *time.Time
	Schema          string
}

type SequenceScanAndPromoteStalledResult struct {
	Continue       bool
	LastSeqKey     string
	PromotedQueues []string
}

type WorkflowCancelParams struct {
	CancelAttemptedAt time.Time
	ControlTopic      string
	Schema            string
	WorkflowID        string
}

type WorkflowCancelWithFailedDepsManyParams struct {
	JobID                []int64
	Schema               string
	WorkflowDepsFailedAt time.Time
	WorkflowIDs          []string
}

type WorkflowGetPendingIDsParams struct {
	LastWorkflowID string
	LimitCount     int32
	Schema         string
	WorkflowID     string
}

type WorkflowGetPendingIDsRow struct {
	JobID      int64
	Task       string
	WorkflowID string
}

type WorkflowJobGetByTaskNameParams struct {
	Schema     string
	TaskName   string
	WorkflowID string
}

type WorkflowJobListParams struct {
	PaginationLimit  int
	PaginationOffset int
	Schema           string
	WorkflowID       string
}

type WorkflowListParams struct {
	After           string
	Max             int
	PaginationLimit int
	Schema          string
}

type WorkflowListItem struct {
	CountAvailable  int
	CountCancelled  int
	CountCompleted  int
	CountDiscarded  int
	CountFailedDeps int
	CountPending    int
	CountRetryable  int
	CountRunning    int
	CountScheduled  int
	ID              string
	CreatedAt       time.Time
	UpdatedAt       time.Time
	Name            *string
	Metadata        []byte
}

type WorkflowLoadDepTasksAndIDsParams struct {
	Recursive  bool
	Task       string
	Schema     string
	TaskNames  []string
	WorkflowID string
}

type WorkflowTask struct {
	ID         int64
	State      rivertype.JobState
	Task       string
	WorkflowID string
	TaskName   string
	Metadata   []byte
	Deps       []string
}

type WorkflowTaskWithJob struct {
	Deps       []string
	WorkflowID string
	Task       *WorkflowTask
	Job        *rivertype.JobRow
}

type WorkflowLoadJobsWithDepsParams struct {
	JobIds     []int64
	Schema     string
	WorkflowID string
}

type WorkflowLoadTaskWithDepsParams struct {
	Schema     string
	Task       string
	TaskName   string
	WorkflowID string
}

type WorkflowLoadTasksByNamesParams struct {
	Schema     string
	TaskNames  []string
	WorkflowID string
}

type WorkflowRetryParams struct {
	Mode         string
	Now          time.Time
	ResetHistory bool
	Schema       string
	WorkflowID   string
}

type WorkflowRetryLockAndCheckRunningParams struct {
	Schema     string
	WorkflowID string
}

type WorkflowRetryLockAndCheckRunningResult struct {
	CanRetry         bool
	WorkflowIsActive bool
}

type WorkflowStageJobsByIDManyParams struct {
	ID               []int64
	Schema           string
	WorkflowIDs      []string
	WorkflowStagedAt time.Time
}

type QueueCreateOrSetUpdatedAtParams struct {
	Metadata  []byte
	Name      string
	Now       *time.Time
	PausedAt  *time.Time
	Schema    string
	UpdatedAt *time.Time
}

type QueueDeleteExpiredParams struct {
	Max              int
	Schema           string
	UpdatedAtHorizon time.Time
}

type QueueGetParams struct {
	Name   string
	Schema string
}

type QueueListParams struct {
	Max    int
	Schema string
}

type QueueNameListParams struct {
	After   string
	Exclude []string
	Match   string
	Max     int
	Schema  string
}

type QueuePauseParams struct {
	Name   string
	Now    *time.Time
	Schema string
}

type QueueResumeParams struct {
	Name   string
	Now    *time.Time
	Schema string
}

type QueueUpdateParams struct {
	Metadata         []byte
	MetadataDoUpdate bool
	Name             string
	Schema           string
}

type Row interface {
	Scan(dest ...any) error
}

type IndexReindexParams struct {
	Index  string
	Schema string
}

type Schema struct {
	Name string
}

type SchemaCreateParams struct {
	Schema string
}

type SchemaDropParams struct {
	Schema string
}

type SchemaGetExpiredParams struct {
	BeforeName string
	Prefix     string
}

type TableExistsParams struct {
	Schema string
	Table  string
}

type TableTruncateParams struct {
	Schema string
	Table  []string
}

// MigrationLineMainTruncateTables is a shared helper that produces tables to
// truncate for the main migration line. It's reused across all drivers.
//
// API is not stable. DO NOT USE.
func MigrationLineMainTruncateTables(version int) []string {
	switch version {
	case 1:
		return nil // don't truncate `river_migrate`
	case 2, 3:
		return []string{"river_job", "river_leader"}
	case 4:
		return []string{"river_job", "river_leader", "river_queue"}
	case 5, 6:
		return []string{"river_job", "river_leader", "river_queue", "river_client", "river_client_queue"}
	}

	// 0 (zero value), and latest versions.
	return []string{"river_job", "river_leader", "river_queue", "river_client", "river_client_queue", "river_job_dead_letter", "river_periodic_job", "river_producer", "river_sequence", "river_workflow"}
}
