package river

import (
	"context"
	"errors"

	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/rivertype"
)

var errDeadLetterNotImplemented = errors.New("river: dead letter operations are not implemented for the configured driver")

type HookJobDeadLetterMove interface {
	IsHook() bool
	JobDeadLetterMove(ctx context.Context, job *rivertype.JobRow) error
}

type HookJobDeadLetterMoveFunc func(ctx context.Context, job *rivertype.JobRow) error

func (f HookJobDeadLetterMoveFunc) IsHook() bool { return true }

func (f HookJobDeadLetterMoveFunc) JobDeadLetterMove(ctx context.Context, job *rivertype.JobRow) error {
	return f(ctx, job)
}

func (c *Client[TTx]) JobDeadLetterGet(ctx context.Context, jobID int64) (*rivertype.JobRow, error) {
	return c.jobDeadLetterGet(ctx, c.driver.GetExecutor(), jobID)
}

func (c *Client[TTx]) JobDeadLetterGetTx(ctx context.Context, tx TTx, jobID int64) (*rivertype.JobRow, error) {
	return c.jobDeadLetterGet(ctx, c.driver.UnwrapExecutor(tx), jobID)
}

func (c *Client[TTx]) jobDeadLetterGet(ctx context.Context, exec riverdriver.Executor, jobID int64) (*rivertype.JobRow, error) {
	job, err := exec.JobDeadLetterGetByID(ctx, &riverdriver.JobDeadLetterGetByIDParams{
		ID:     jobID,
		Schema: c.config.Schema,
	})
	if errors.Is(err, riverdriver.ErrNotImplemented) {
		return nil, errDeadLetterNotImplemented
	}
	return job, err
}

func (c *Client[TTx]) JobDeadLetterRetry(ctx context.Context, jobID int64) (*rivertype.JobInsertResult, error) {
	return c.jobDeadLetterRetry(ctx, c.driver.GetExecutor(), jobID)
}

func (c *Client[TTx]) JobDeadLetterRetryTx(ctx context.Context, tx TTx, jobID int64) (*rivertype.JobInsertResult, error) {
	return c.jobDeadLetterRetry(ctx, c.driver.UnwrapExecutor(tx), jobID)
}

func (c *Client[TTx]) JobDeadLetterDelete(ctx context.Context, jobID int64) (*rivertype.JobRow, error) {
	return c.jobDeadLetterDelete(ctx, c.driver.GetExecutor(), jobID)
}

func (c *Client[TTx]) JobDeadLetterDeleteTx(ctx context.Context, tx TTx, jobID int64) (*rivertype.JobRow, error) {
	return c.jobDeadLetterDelete(ctx, c.driver.UnwrapExecutor(tx), jobID)
}

func (c *Client[TTx]) jobDeadLetterDelete(ctx context.Context, exec riverdriver.Executor, jobID int64) (*rivertype.JobRow, error) {
	job, err := exec.JobDeadLetterDeleteByID(ctx, &riverdriver.JobDeadLetterDeleteByIDParams{
		ID:     jobID,
		Schema: c.config.Schema,
	})
	if errors.Is(err, riverdriver.ErrNotImplemented) {
		return nil, errDeadLetterNotImplemented
	}
	return job, err
}

func (c *Client[TTx]) JobDeadLetterList(ctx context.Context, max int) ([]*rivertype.JobRow, error) {
	return c.jobDeadLetterList(ctx, c.driver.GetExecutor(), max)
}

func (c *Client[TTx]) JobDeadLetterListTx(ctx context.Context, tx TTx, max int) ([]*rivertype.JobRow, error) {
	return c.jobDeadLetterList(ctx, c.driver.UnwrapExecutor(tx), max)
}

func (c *Client[TTx]) jobDeadLetterList(ctx context.Context, exec riverdriver.Executor, max int) ([]*rivertype.JobRow, error) {
	jobs, err := exec.JobDeadLetterGetAll(ctx, &riverdriver.JobDeadLetterGetAllParams{
		Max:    max,
		Schema: c.config.Schema,
	})
	if errors.Is(err, riverdriver.ErrNotImplemented) {
		return nil, errDeadLetterNotImplemented
	}
	return jobs, err
}

func (c *Client[TTx]) jobDeadLetterRetry(ctx context.Context, exec riverdriver.Executor, jobID int64) (*rivertype.JobInsertResult, error) {
	job, err := exec.JobDeadLetterMoveByID(ctx, &riverdriver.JobDeadLetterMoveByIDParams{
		ID:     jobID,
		Schema: c.config.Schema,
	})
	if errors.Is(err, riverdriver.ErrNotImplemented) {
		return nil, errDeadLetterNotImplemented
	}
	if err != nil {
		return nil, err
	}

	return &rivertype.JobInsertResult{Job: job}, nil
}
