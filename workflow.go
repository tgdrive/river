package river

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"sort"
	"time"

	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/rivertype"
)

var errWorkflowNotImplemented = errors.New("river: workflow operations are not implemented for the configured driver")

type WorkflowOpts struct {
	ID       string
	Name     string
	Metadata []byte

	IgnoreCancelledDeps bool
	IgnoreDiscardedDeps bool
	IgnoreDeletedDeps   bool
}

type WorkflowTaskOpts struct {
	Deps     []string
	Metadata []byte

	IgnoreCancelledDeps bool
	IgnoreDiscardedDeps bool
	IgnoreDeletedDeps   bool
}

type WorkflowTask struct {
	Name       string
	Args       JobArgs
	InsertOpts *InsertOpts
	Opts       *WorkflowTaskOpts
}

type WorkflowPrepareResult struct {
	Jobs []InsertManyParams
}

type WorkflowCancelResult struct {
	CancelledJobs []*rivertype.JobRow
}

type WorkflowTasks struct {
	ByName map[string]*WorkflowTaskWithJob
}

type WorkflowTaskWithJob struct {
	TaskName string
	Deps     []string
	Job      *rivertype.JobRow
}

func (w *WorkflowTaskWithJob) Output(v any) error {
	if w == nil || w.Job == nil {
		return &TaskHasNoOutputError{TaskName: ""}
	}
	var metadata map[string]json.RawMessage
	if err := json.Unmarshal(w.Job.Metadata, &metadata); err != nil {
		return err
	}
	raw, ok := metadata["output"]
	if !ok {
		return &TaskHasNoOutputError{TaskName: w.TaskName}
	}
	return json.Unmarshal(raw, v)
}

func (t *WorkflowTasks) Count() int {
	if t == nil || t.ByName == nil {
		return 0
	}
	return len(t.ByName)
}

func (t *WorkflowTasks) Get(taskName string) *WorkflowTaskWithJob {
	if t == nil || t.ByName == nil {
		return nil
	}
	return t.ByName[taskName]
}

func (t *WorkflowTasks) Names() []string {
	if t == nil || t.ByName == nil {
		return nil
	}
	names := make([]string, 0, len(t.ByName))
	for name := range t.ByName {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func (t *WorkflowTasks) Output(taskName string, v any) error {
	task := t.Get(taskName)
	if task == nil {
		return fmt.Errorf("task %q not found", taskName)
	}
	return task.Output(v)
}

type WorkflowLoadAllOpts struct{}

type WorkflowLoadDepsOpts struct {
	Recursive bool
}

type WorkflowRetryOpts struct {
	Mode         WorkflowRetryMode
	ResetHistory bool
}

type WorkflowRetryMode string

const (
	WorkflowRetryModeAll                 WorkflowRetryMode = "all"
	WorkflowRetryModeFailedOnly          WorkflowRetryMode = "failed_only"
	WorkflowRetryModeFailedAndDownstream WorkflowRetryMode = "failed_and_downstream"
)

type WorkflowRetryResult struct {
	Jobs        []*rivertype.JobRow
	RetriedJobs []*rivertype.JobRow
}

type WorkflowRetryStillActiveError struct{ WorkflowID string }

func (e *WorkflowRetryStillActiveError) Error() string {
	if e.WorkflowID == "" {
		return "workflow still has active jobs and cannot be retried"
	}
	return fmt.Sprintf("workflow %q still has active jobs and cannot be retried", e.WorkflowID)
}

func (e *WorkflowRetryStillActiveError) Is(target error) bool {
	_, ok := target.(*WorkflowRetryStillActiveError)
	return ok
}

type DuplicateTaskError struct{ TaskName string }

func (e *DuplicateTaskError) Error() string {
	return fmt.Sprintf("duplicate workflow task name %q", e.TaskName)
}

func (e *DuplicateTaskError) Is(target error) bool {
	_, ok := target.(*DuplicateTaskError)
	return ok
}

type MissingDependencyError struct {
	TaskName string
	DepName  string
}

func (e *MissingDependencyError) Error() string {
	return fmt.Sprintf("workflow task %q depends on missing task %q", e.TaskName, e.DepName)
}

func (e *MissingDependencyError) Is(target error) bool {
	_, ok := target.(*MissingDependencyError)
	return ok
}

type DependencyCycleError struct{ DepStack []string }

func (e *DependencyCycleError) Error() string {
	return fmt.Sprintf("workflow has dependency cycle: %v", e.DepStack)
}

func (e *DependencyCycleError) Is(target error) bool {
	_, ok := target.(*DependencyCycleError)
	return ok
}

type TaskHasNoOutputError struct{ TaskName string }

func (e *TaskHasNoOutputError) Error() string {
	if e.TaskName == "" {
		return "workflow task has no output"
	}
	return fmt.Sprintf("workflow task %q has no output", e.TaskName)
}

func (e *TaskHasNoOutputError) Is(target error) bool {
	_, ok := target.(*TaskHasNoOutputError)
	return ok
}

type WorkflowT[TTx any] struct {
	client *Client[TTx]
	opts   *WorkflowOpts
	tasks  []WorkflowTask
}

type Workflow = WorkflowT[any]

func (c *Client[TTx]) NewWorkflow(opts *WorkflowOpts) *WorkflowT[TTx] {
	return &WorkflowT[TTx]{client: c, opts: opts}
}

func (c *Client[TTx]) WorkflowFromExisting(job *rivertype.JobRow, opts *WorkflowOpts) (*WorkflowT[TTx], error) {
	workflowOpts := opts
	if workflowOpts == nil {
		workflowOpts = &WorkflowOpts{}
	}

	if job == nil {
		return nil, errors.New("job cannot be nil")
	}

	metadata := map[string]json.RawMessage{}
	if len(job.Metadata) > 0 {
		if err := json.Unmarshal(job.Metadata, &metadata); err != nil {
			return nil, err
		}
	}

	if workflowOpts.ID == "" {
		if workflowID, ok := metadata["workflow_id"]; ok {
			if err := json.Unmarshal(workflowID, &workflowOpts.ID); err != nil {
				return nil, err
			}
		}
		if workflowOpts.ID == "" {
			return nil, errors.New("job is not part of a workflow")
		}
	}

	if workflowOpts.Name == "" {
		if workflowName, ok := metadata["workflow_name"]; ok {
			if err := json.Unmarshal(workflowName, &workflowOpts.Name); err != nil {
				return nil, err
			}
		}
	}

	return &WorkflowT[TTx]{client: c, opts: workflowOpts}, nil
}

func (c *Client[TTx]) WorkflowRetry(ctx context.Context, workflowID string, opts *WorkflowRetryOpts) (*WorkflowRetryResult, error) {
	return c.NewWorkflow(&WorkflowOpts{ID: workflowID}).Retry(ctx, opts)
}

// Deprecated: Use Workflow.Prepare instead.
func (c *Client[TTx]) WorkflowPrepare(ctx context.Context, workflow *Workflow) (*WorkflowPrepareResult, error) {
	if workflow == nil {
		return nil, errors.New("workflow cannot be nil")
	}
	return workflow.Prepare(ctx)
}

// Deprecated: Use Workflow.PrepareTx instead.
func (c *Client[TTx]) WorkflowPrepareTx(ctx context.Context, tx TTx, workflow *Workflow) (*WorkflowPrepareResult, error) {
	if workflow == nil {
		return nil, errors.New("workflow cannot be nil")
	}
	return workflow.PrepareTx(ctx, tx)
}

func (c *Client[TTx]) WorkflowRetryTx(ctx context.Context, tx TTx, workflowID string, opts *WorkflowRetryOpts) (*WorkflowRetryResult, error) {
	exec := c.driver.UnwrapExecutor(tx)
	canRetry, err := exec.WorkflowRetryLockAndCheckRunning(ctx, &riverdriver.WorkflowRetryLockAndCheckRunningParams{Schema: c.config.Schema, WorkflowID: workflowID})
	if errors.Is(err, riverdriver.ErrNotImplemented) {
		return nil, errWorkflowNotImplemented
	}
	if err != nil {
		return nil, err
	}
	if !canRetry.CanRetry {
		return nil, &WorkflowRetryStillActiveError{WorkflowID: workflowID}
	}

	mode := WorkflowRetryModeAll
	resetHistory := false
	if opts != nil {
		if opts.Mode != "" {
			mode = opts.Mode
		}
		resetHistory = opts.ResetHistory
	}

	jobs, err := exec.WorkflowRetry(ctx, &riverdriver.WorkflowRetryParams{
		Mode:         string(mode),
		Now:          c.baseService.Time.NowUTC(),
		ResetHistory: resetHistory,
		Schema:       c.config.Schema,
		WorkflowID:   workflowID,
	})
	if err != nil {
		return nil, err
	}
	return &WorkflowRetryResult{Jobs: jobs, RetriedJobs: jobs}, nil
}

func (c *Client[TTx]) WorkflowCancel(ctx context.Context, workflowID string) (*WorkflowCancelResult, error) {
	return c.workflowCancel(ctx, c.driver.GetExecutor(), workflowID, true)
}

func (c *Client[TTx]) WorkflowCancelTx(ctx context.Context, tx TTx, workflowID string) (*WorkflowCancelResult, error) {
	return c.workflowCancel(ctx, c.driver.UnwrapExecutor(tx), workflowID, false)
}

func (c *Client[TTx]) workflowCancel(ctx context.Context, exec riverdriver.Executor, workflowID string, notifyRunning bool) (*WorkflowCancelResult, error) {
	jobs, err := exec.WorkflowCancel(ctx, &riverdriver.WorkflowCancelParams{Schema: c.config.Schema, WorkflowID: workflowID})
	if errors.Is(err, riverdriver.ErrNotImplemented) {
		return nil, errWorkflowNotImplemented
	}
	if err != nil {
		return nil, err
	}

	workflowJobs, err := exec.WorkflowJobList(ctx, &riverdriver.WorkflowJobListParams{Schema: c.config.Schema, WorkflowID: workflowID})
	if errors.Is(err, riverdriver.ErrNotImplemented) {
		return nil, errWorkflowNotImplemented
	}
	if err != nil {
		return nil, err
	}

	for _, job := range workflowJobs {
		if job == nil || job.State != rivertype.JobStateRunning {
			continue
		}

		cancelledJob, err := c.jobCancel(ctx, exec, job.ID)
		if err != nil {
			return nil, err
		}
		jobs = append(jobs, cancelledJob)

		if notifyRunning {
			c.notifyProducerWithoutListenerQueueControlEvent(cancelledJob.Queue, &controlEventPayload{
				Action: controlActionCancel,
				JobID:  cancelledJob.ID,
				Queue:  cancelledJob.Queue,
			})
		}
	}

	return &WorkflowCancelResult{CancelledJobs: jobs}, nil
}

func (w *WorkflowT[TTx]) Add(taskName string, args JobArgs, insertOpts *InsertOpts, opts *WorkflowTaskOpts) WorkflowTask {
	task := WorkflowTask{Name: taskName, Args: args, InsertOpts: insertOpts, Opts: opts}
	w.tasks = append(w.tasks, task)
	return task
}

func (w *WorkflowT[TTx]) AddSafely(taskName string, args JobArgs, insertOpts *InsertOpts, opts *WorkflowTaskOpts) (WorkflowTask, error) {
	if taskName == "" {
		return WorkflowTask{}, errors.New("task name cannot be empty")
	}
	for _, task := range w.tasks {
		if task.Name == taskName {
			return WorkflowTask{}, &DuplicateTaskError{TaskName: taskName}
		}
	}
	return w.Add(taskName, args, insertOpts, opts), nil
}

func (w *WorkflowT[TTx]) ID() string {
	if w.opts == nil {
		return ""
	}
	return w.opts.ID
}

func (w *WorkflowT[TTx]) Prepare(_ context.Context) (*WorkflowPrepareResult, error) {
	if w.opts == nil {
		w.opts = &WorkflowOpts{}
	}
	if w.opts.ID == "" {
		w.opts.ID = fmt.Sprintf("wf_%d", time.Now().UTC().UnixNano())
	}

	taskNames := make(map[string]struct{}, len(w.tasks))
	for _, task := range w.tasks {
		if _, ok := taskNames[task.Name]; ok {
			return nil, &DuplicateTaskError{TaskName: task.Name}
		}
		taskNames[task.Name] = struct{}{}
	}

	jobs := make([]InsertManyParams, 0, len(w.tasks))
	for _, task := range w.tasks {
		deps := []string{}
		if task.Opts != nil {
			deps = task.Opts.Deps
		}
		for _, dep := range deps {
			if _, ok := taskNames[dep]; !ok {
				return nil, &MissingDependencyError{TaskName: task.Name, DepName: dep}
			}
		}

		if _, ok := task.Args.(JobArgsWithSequence); ok {
			return nil, fmt.Errorf("workflow task %q cannot use sequence options", task.Name)
		}

		insertOpts := &InsertOpts{}
		if task.InsertOpts != nil {
			copied := *task.InsertOpts
			insertOpts = &copied
		}

		metadataMap := map[string]any{}
		if len(insertOpts.Metadata) > 0 {
			_ = json.Unmarshal(insertOpts.Metadata, &metadataMap)
		}
		if len(w.opts.Metadata) > 0 {
			workflowMetadata := map[string]any{}
			if err := json.Unmarshal(w.opts.Metadata, &workflowMetadata); err == nil {
				maps.Copy(metadataMap, workflowMetadata)
			}
		}

		ignoreCancelledDeps := w.opts.IgnoreCancelledDeps
		ignoreDiscardedDeps := w.opts.IgnoreDiscardedDeps
		ignoreDeletedDeps := w.opts.IgnoreDeletedDeps
		if task.Opts != nil {
			ignoreCancelledDeps = ignoreCancelledDeps || task.Opts.IgnoreCancelledDeps
			ignoreDiscardedDeps = ignoreDiscardedDeps || task.Opts.IgnoreDiscardedDeps
			ignoreDeletedDeps = ignoreDeletedDeps || task.Opts.IgnoreDeletedDeps
		}

		metadataMap["workflow_id"] = w.opts.ID
		if w.opts.Name != "" {
			metadataMap["workflow_name"] = w.opts.Name
		}
		metadataMap["workflow_task_name"] = task.Name
		metadataMap["workflow_deps"] = deps
		metadataMap["workflow_ignore_cancelled_deps"] = ignoreCancelledDeps
		metadataMap["workflow_ignore_discarded_deps"] = ignoreDiscardedDeps
		metadataMap["workflow_ignore_deleted_deps"] = ignoreDeletedDeps
		if task.Opts != nil && len(task.Opts.Metadata) > 0 {
			taskMetadata := map[string]any{}
			if err := json.Unmarshal(task.Opts.Metadata, &taskMetadata); err == nil {
				maps.Copy(metadataMap, taskMetadata)
			}
		}

		metadataBytes, err := json.Marshal(metadataMap)
		if err != nil {
			return nil, err
		}
		insertOpts.Metadata = metadataBytes
		if len(deps) > 0 {
			insertOpts.Pending = true
		}

		jobs = append(jobs, InsertManyParams{Args: task.Args, InsertOpts: insertOpts})
	}

	if err := validateWorkflowAcyclic(w.tasks); err != nil {
		return nil, err
	}

	return &WorkflowPrepareResult{Jobs: jobs}, nil
}

func (w *WorkflowT[TTx]) PrepareTx(ctx context.Context, _ TTx) (*WorkflowPrepareResult, error) {
	return w.Prepare(ctx)
}

func (w *WorkflowT[TTx]) LoadAll(ctx context.Context, _ *WorkflowLoadAllOpts) (*WorkflowTasks, error) {
	return w.loadAllWithExecutor(ctx, w.client.driver.GetExecutor())
}

func (w *WorkflowT[TTx]) LoadAllTx(ctx context.Context, tx TTx, _ *WorkflowLoadAllOpts) (*WorkflowTasks, error) {
	return w.loadAllWithExecutor(ctx, w.client.driver.UnwrapExecutor(tx))
}

func (w *WorkflowT[TTx]) loadAllWithExecutor(ctx context.Context, exec riverdriver.Executor) (*WorkflowTasks, error) {
	rows, err := exec.WorkflowLoadJobsWithDeps(ctx, &riverdriver.WorkflowLoadJobsWithDepsParams{Schema: w.client.config.Schema, WorkflowID: w.ID()})
	if errors.Is(err, riverdriver.ErrNotImplemented) {
		return nil, errWorkflowNotImplemented
	}
	if err != nil {
		return nil, err
	}

	out := &WorkflowTasks{ByName: make(map[string]*WorkflowTaskWithJob, len(rows))}
	for _, row := range rows {
		if row == nil || row.Task == nil || row.Job == nil {
			continue
		}
		out.ByName[row.Task.TaskName] = &WorkflowTaskWithJob{TaskName: row.Task.TaskName, Deps: row.Task.Deps, Job: row.Job}
	}

	return out, nil
}

func (w *WorkflowT[TTx]) LoadTask(ctx context.Context, taskName string) (*WorkflowTaskWithJob, error) {
	tasks, err := w.LoadDeps(ctx, taskName, nil)
	if err != nil {
		return nil, err
	}
	task := tasks.Get(taskName)
	if task == nil {
		return nil, rivertype.ErrNotFound
	}
	return task, nil
}

func (w *WorkflowT[TTx]) LoadTaskTx(ctx context.Context, tx TTx, taskName string) (*WorkflowTaskWithJob, error) {
	tasks, err := w.LoadDepsTx(ctx, tx, taskName, nil)
	if err != nil {
		return nil, err
	}
	task := tasks.Get(taskName)
	if task == nil {
		return nil, rivertype.ErrNotFound
	}
	return task, nil
}

func (w *WorkflowT[TTx]) LoadDeps(ctx context.Context, taskName string, opts *WorkflowLoadDepsOpts) (*WorkflowTasks, error) {
	if opts == nil {
		opts = &WorkflowLoadDepsOpts{}
	}
	return w.loadDeps(ctx, w.client.driver.GetExecutor(), taskName, opts)
}

func (w *WorkflowT[TTx]) LoadDepsTx(ctx context.Context, tx TTx, taskName string, opts *WorkflowLoadDepsOpts) (*WorkflowTasks, error) {
	if opts == nil {
		opts = &WorkflowLoadDepsOpts{}
	}
	return w.loadDeps(ctx, w.client.driver.UnwrapExecutor(tx), taskName, opts)
}

func (w *WorkflowT[TTx]) loadDeps(ctx context.Context, exec riverdriver.Executor, taskName string, opts *WorkflowLoadDepsOpts) (*WorkflowTasks, error) {
	task, err := exec.WorkflowLoadTaskWithDeps(ctx, &riverdriver.WorkflowLoadTaskWithDepsParams{Schema: w.client.config.Schema, WorkflowID: w.ID(), TaskName: taskName})
	if errors.Is(err, riverdriver.ErrNotImplemented) {
		return nil, errWorkflowNotImplemented
	}
	if err != nil {
		return nil, err
	}

	out := &WorkflowTasks{ByName: map[string]*WorkflowTaskWithJob{}}
	if task != nil && task.Task != nil && task.Job != nil {
		out.ByName[task.Task.TaskName] = &WorkflowTaskWithJob{TaskName: task.Task.TaskName, Deps: task.Task.Deps, Job: task.Job}
	}
	if task == nil || task.Task == nil || len(task.Task.Deps) == 0 {
		return out, nil
	}

	deps, err := exec.WorkflowLoadTasksByNames(ctx, &riverdriver.WorkflowLoadTasksByNamesParams{Schema: w.client.config.Schema, WorkflowID: w.ID(), TaskNames: task.Task.Deps})
	if err != nil {
		return nil, err
	}
	nextLevel := make([]string, 0)
	for _, dep := range deps {
		if dep == nil || dep.TaskName == "" {
			continue
		}
		job, err := exec.WorkflowJobGetByTaskName(ctx, &riverdriver.WorkflowJobGetByTaskNameParams{Schema: w.client.config.Schema, WorkflowID: w.ID(), TaskName: dep.TaskName})
		if err != nil {
			return nil, err
		}
		out.ByName[dep.TaskName] = &WorkflowTaskWithJob{TaskName: dep.TaskName, Deps: dep.Deps, Job: job}
		if opts != nil && opts.Recursive {
			nextLevel = append(nextLevel, dep.TaskName)
		}
	}

	if opts != nil && opts.Recursive {
		for _, depTaskName := range nextLevel {
			depTasks, err := w.loadDeps(ctx, exec, depTaskName, opts)
			if err != nil {
				return nil, err
			}
			maps.Copy(out.ByName, depTasks.ByName)
		}
	}

	return out, nil
}

func validateWorkflowAcyclic(tasks []WorkflowTask) error {
	adj := make(map[string][]string, len(tasks))
	for _, task := range tasks {
		deps := []string{}
		if task.Opts != nil {
			deps = append(deps, task.Opts.Deps...)
		}
		adj[task.Name] = deps
	}

	const (
		unvisited = 0
		visiting  = 1
		visited   = 2
	)

	state := make(map[string]int, len(tasks))
	stack := make([]string, 0, len(tasks))

	var dfs func(string) error
	dfs = func(node string) error {
		state[node] = visiting
		stack = append(stack, node)

		for _, dep := range adj[node] {
			switch state[dep] {
			case visiting:
				cycle := append([]string{}, stack...)
				cycle = append(cycle, dep)
				return &DependencyCycleError{DepStack: cycle}
			case unvisited:
				if err := dfs(dep); err != nil {
					return err
				}
			}
		}

		stack = stack[:len(stack)-1]
		state[node] = visited
		return nil
	}

	for node := range adj {
		if state[node] == unvisited {
			if err := dfs(node); err != nil {
				return err
			}
		}
	}

	return nil
}

func (w *WorkflowT[TTx]) LoadOutput(ctx context.Context, taskName string, v any) error {
	return w.loadOutputWithExecutor(ctx, w.client.driver.GetExecutor(), taskName, v)
}

func (w *WorkflowT[TTx]) LoadOutputTx(ctx context.Context, tx TTx, taskName string, v any) error {
	return w.loadOutputWithExecutor(ctx, w.client.driver.UnwrapExecutor(tx), taskName, v)
}

func (w *WorkflowT[TTx]) LoadOutputByJob(ctx context.Context, job *rivertype.JobRow, v any) error {
	taskName, err := workflowTaskNameFromJob(job)
	if err != nil {
		return err
	}
	return w.LoadOutput(ctx, taskName, v)
}

func (w *WorkflowT[TTx]) LoadOutputByJobTx(ctx context.Context, tx TTx, job *rivertype.JobRow, v any) error {
	taskName, err := workflowTaskNameFromJob(job)
	if err != nil {
		return err
	}
	return w.LoadOutputTx(ctx, tx, taskName, v)
}

func (w *WorkflowT[TTx]) loadOutputWithExecutor(ctx context.Context, exec riverdriver.Executor, taskName string, v any) error {
	job, err := exec.WorkflowJobGetByTaskName(ctx, &riverdriver.WorkflowJobGetByTaskNameParams{Schema: w.client.config.Schema, WorkflowID: w.ID(), TaskName: taskName})
	if errors.Is(err, riverdriver.ErrNotImplemented) {
		return errWorkflowNotImplemented
	}
	if err != nil {
		return err
	}

	var metadata map[string]json.RawMessage
	if err := json.Unmarshal(job.Metadata, &metadata); err != nil {
		return err
	}
	raw, ok := metadata["output"]
	if !ok {
		return &TaskHasNoOutputError{TaskName: taskName}
	}
	return json.Unmarshal(raw, v)
}

func (w *WorkflowT[TTx]) LoadDepsByJob(ctx context.Context, job *rivertype.JobRow, opts *WorkflowLoadDepsOpts) (*WorkflowTasks, error) {
	taskName, err := workflowTaskNameFromJob(job)
	if err != nil {
		return nil, err
	}
	return w.LoadDeps(ctx, taskName, opts)
}

func (w *WorkflowT[TTx]) LoadDepsByJobTx(ctx context.Context, tx TTx, job *rivertype.JobRow, opts *WorkflowLoadDepsOpts) (*WorkflowTasks, error) {
	taskName, err := workflowTaskNameFromJob(job)
	if err != nil {
		return nil, err
	}
	return w.LoadDepsTx(ctx, tx, taskName, opts)
}

func (w *WorkflowT[TTx]) Retry(ctx context.Context, opts *WorkflowRetryOpts) (*WorkflowRetryResult, error) {
	return w.retryWithExecutor(ctx, w.client.driver.GetExecutor(), opts)
}

func (w *WorkflowT[TTx]) RetryTx(ctx context.Context, tx TTx, opts *WorkflowRetryOpts) (*WorkflowRetryResult, error) {
	return w.retryWithExecutor(ctx, w.client.driver.UnwrapExecutor(tx), opts)
}

func (w *WorkflowT[TTx]) retryWithExecutor(ctx context.Context, exec riverdriver.Executor, opts *WorkflowRetryOpts) (*WorkflowRetryResult, error) {
	canRetry, err := exec.WorkflowRetryLockAndCheckRunning(ctx, &riverdriver.WorkflowRetryLockAndCheckRunningParams{Schema: w.client.config.Schema, WorkflowID: w.ID()})
	if errors.Is(err, riverdriver.ErrNotImplemented) {
		return nil, errWorkflowNotImplemented
	}
	if err != nil {
		return nil, err
	}
	if !canRetry.CanRetry {
		return nil, &WorkflowRetryStillActiveError{WorkflowID: w.ID()}
	}

	mode := WorkflowRetryModeAll
	resetHistory := false
	if opts != nil {
		if opts.Mode != "" {
			mode = opts.Mode
		}
		resetHistory = opts.ResetHistory
	}
	jobs, err := exec.WorkflowRetry(ctx, &riverdriver.WorkflowRetryParams{Mode: string(mode), Now: w.client.baseService.Time.NowUTC(), ResetHistory: resetHistory, Schema: w.client.config.Schema, WorkflowID: w.ID()})
	if err != nil {
		return nil, err
	}
	return &WorkflowRetryResult{Jobs: jobs, RetriedJobs: jobs}, nil
}

func workflowTaskNameFromJob(job *rivertype.JobRow) (string, error) {
	if job == nil {
		return "", errors.New("job cannot be nil")
	}
	var metadata map[string]json.RawMessage
	if err := json.Unmarshal(job.Metadata, &metadata); err != nil {
		return "", err
	}
	var taskName string
	if raw, ok := metadata["workflow_task_name"]; ok {
		if err := json.Unmarshal(raw, &taskName); err != nil {
			return "", err
		}
	}
	if taskName == "" {
		return "", errors.New("job does not contain workflow task name")
	}
	return taskName, nil
}
