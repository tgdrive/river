package riverbatch

import (
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"sync"

	"github.com/riverqueue/river/rivertype"
)

type MultiError struct {
	mu   sync.RWMutex
	byID map[int64]error
	jobs []*rivertype.JobRow
}

func NewMultiError() *MultiError {
	return &MultiError{byID: make(map[int64]error)}
}

func (e *MultiError) Add(job *rivertype.JobRow, err error) {
	if job == nil {
		return
	}
	e.AddByID(job.ID, err)
}

func (e *MultiError) AddByID(jobID int64, err error) {
	if err == nil {
		return
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	e.byID[jobID] = err
}

func (e *MultiError) setJobs(jobs []*rivertype.JobRow) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.jobs = append([]*rivertype.JobRow(nil), jobs...)
}

func (e *MultiError) Error() string {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if len(e.byID) == 0 {
		return ""
	}

	ids := make([]int64, 0, len(e.byID))
	for id := range e.byID {
		ids = append(ids, id)
	}
	slices.Sort(ids)

	parts := make([]string, 0, len(ids))
	for _, id := range ids {
		parts = append(parts, fmt.Sprintf("%d: %v", id, e.byID[id]))
	}

	return strings.Join(parts, "; ")
}

func (e *MultiError) Get(job *rivertype.JobRow) error {
	if job == nil {
		return nil
	}
	return e.GetByID(job.ID)
}

func (e *MultiError) GetByID(jobID int64) error {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.byID[jobID]
}

func (e *MultiError) Unwrap() error {
	e.mu.RLock()
	defer e.mu.RUnlock()

	errs := make([]error, 0, len(e.byID))
	for _, err := range e.byID {
		errs = append(errs, err)
	}

	return errors.Join(errs...)
}

func (e *MultiError) Is(target error) bool {
	if target == nil {
		return false
	}
	if _, ok := target.(*MultiError); ok {
		return true
	}
	return errors.Is(e.Unwrap(), target)
}

func (e *MultiError) ErrorsByID() map[int64]error {
	e.mu.RLock()
	defer e.mu.RUnlock()
	out := make(map[int64]error, len(e.byID))
	maps.Copy(out, e.byID)
	return out
}

func (e *MultiError) Jobs() []*rivertype.JobRow {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return append([]*rivertype.JobRow(nil), e.jobs...)
}
