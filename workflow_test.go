package river

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/rivertype"
)

type workflowArgs struct{}

func (workflowArgs) Kind() string { return "workflow_args" }

type workflowSequenceArgs struct{}

func (workflowSequenceArgs) Kind() string               { return "workflow_sequence_args" }
func (workflowSequenceArgs) SequenceOpts() SequenceOpts { return SequenceOpts{} }

func TestWorkflow_PrepareMetadataAndPending(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	client := &Client[any]{config: &Config{Schema: "river"}}
	workflow := client.NewWorkflow(&WorkflowOpts{ID: "wf_test"})

	a := workflow.Add("a", workflowArgs{}, nil, nil)
	workflow.Add("b", workflowArgs{}, nil, &WorkflowTaskOpts{Deps: []string{a.Name}})

	prepared, err := workflow.Prepare(ctx)
	require.NoError(t, err)
	require.Len(t, prepared.Jobs, 2)

	var mdA map[string]any
	require.NoError(t, json.Unmarshal(prepared.Jobs[0].InsertOpts.Metadata, &mdA))
	require.Equal(t, "wf_test", mdA["workflow_id"])
	require.Equal(t, "a", mdA["workflow_task_name"])

	var mdB map[string]any
	require.NoError(t, json.Unmarshal(prepared.Jobs[1].InsertOpts.Metadata, &mdB))
	require.Equal(t, "wf_test", mdB["workflow_id"])
	require.Equal(t, "b", mdB["workflow_task_name"])
	require.True(t, prepared.Jobs[1].InsertOpts.Pending)
}

func TestWorkflow_PrepareValidation(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	client := &Client[any]{config: &Config{Schema: "river"}}

	t.Run("DuplicateTask", func(t *testing.T) {
		workflow := client.NewWorkflow(&WorkflowOpts{ID: "wf_dup"})
		workflow.Add("a", workflowArgs{}, nil, nil)
		workflow.Add("a", workflowArgs{}, nil, nil)

		_, err := workflow.Prepare(ctx)
		require.Error(t, err)
		duplicateTaskError := &DuplicateTaskError{}
		ok := errors.As(err, &duplicateTaskError)
		require.True(t, ok)
	})

	t.Run("MissingDependency", func(t *testing.T) {
		workflow := client.NewWorkflow(&WorkflowOpts{ID: "wf_missing"})
		workflow.Add("a", workflowArgs{}, nil, &WorkflowTaskOpts{Deps: []string{"b"}})

		_, err := workflow.Prepare(ctx)
		require.Error(t, err)
		missingDependencyError := &MissingDependencyError{}
		ok := errors.As(err, &missingDependencyError)
		require.True(t, ok)
	})

	t.Run("DependencyCycle", func(t *testing.T) {
		workflow := client.NewWorkflow(&WorkflowOpts{ID: "wf_cycle"})
		workflow.Add("a", workflowArgs{}, nil, &WorkflowTaskOpts{Deps: []string{"b"}})
		workflow.Add("b", workflowArgs{}, nil, &WorkflowTaskOpts{Deps: []string{"a"}})

		_, err := workflow.Prepare(ctx)
		require.Error(t, err)
		dependencyCycleError := &DependencyCycleError{}
		ok := errors.As(err, &dependencyCycleError)
		require.True(t, ok)
	})

	t.Run("SequenceNotAllowed", func(t *testing.T) {
		workflow := client.NewWorkflow(&WorkflowOpts{ID: "wf_seq"})
		workflow.Add("a", workflowSequenceArgs{}, nil, nil)

		_, err := workflow.Prepare(ctx)
		require.Error(t, err)
		require.Contains(t, err.Error(), "cannot use sequence options")
	})
}

func TestWorkflowTasks_OutputAndGet(t *testing.T) {
	t.Parallel()

	metadata := []byte(`{"output":{"result":"ok"}}`)
	tasks := &WorkflowTasks{ByName: map[string]*WorkflowTaskWithJob{
		"task_a": {TaskName: "task_a", Job: &rivertype.JobRow{Metadata: metadata}},
	}}

	require.NotNil(t, tasks.Get("task_a"))
	require.Nil(t, tasks.Get("missing"))

	var out struct {
		Result string `json:"result"`
	}
	require.NoError(t, tasks.Output("task_a", &out))
	require.Equal(t, "ok", out.Result)
	require.Error(t, tasks.Output("missing", &out))
}
