package maintenance

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/riverqueue/river/internal/notifier"
	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/rivershared/baseservice"
	"github.com/riverqueue/river/rivershared/riversharedmaintenance"
	"github.com/riverqueue/river/rivershared/startstop"
	"github.com/riverqueue/river/rivershared/testsignal"
	"github.com/riverqueue/river/rivershared/util/randutil"
	"github.com/riverqueue/river/rivershared/util/serviceutil"
	"github.com/riverqueue/river/rivershared/util/testutil"
	"github.com/riverqueue/river/rivershared/util/timeutil"
	"github.com/riverqueue/river/rivertype"
)

type SequencePromoterTestSignals struct {
	PromotedBatch testsignal.TestSignal[[]string]
}

func (ts *SequencePromoterTestSignals) Init(tb testutil.TestingTB) { ts.PromotedBatch.Init(tb) }

type SequencePromoterConfig struct {
	Interval             time.Duration
	NotifyNonTxJobInsert func(ctx context.Context, res []*rivertype.JobInsertResult)
	Schema               string
}

func (c *SequencePromoterConfig) mustValidate() *SequencePromoterConfig {
	if c.Interval <= 0 {
		panic("SequencePromoterConfig.Interval must be above zero")
	}
	if c.NotifyNonTxJobInsert == nil {
		panic("SequencePromoterConfig.NotifyNonTxJobInsert must be set")
	}
	return c
}

type SequencePromoter struct {
	riversharedmaintenance.QueueMaintainerServiceBase
	startstop.BaseStartStop

	config *SequencePromoterConfig
	exec   riverdriver.Executor

	TestSignals SequencePromoterTestSignals
}

func NewSequencePromoter(archetype *baseservice.Archetype, config *SequencePromoterConfig, exec riverdriver.Executor) *SequencePromoter {
	return baseservice.Init(archetype, &SequencePromoter{
		config: (&SequencePromoterConfig{Interval: cmp.Or(config.Interval, JobSchedulerIntervalDefault), NotifyNonTxJobInsert: config.NotifyNonTxJobInsert, Schema: config.Schema}).mustValidate(),
		exec:   exec,
	})
}

func (s *SequencePromoter) Start(ctx context.Context) error {
	ctx, shouldStart, started, stopped := s.StartInit(ctx)
	if !shouldStart {
		return nil
	}
	go func() {
		started()
		defer stopped()
		ticker := timeutil.NewTickerWithInitialTick(ctx, s.config.Interval)
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
			queues, err := s.runOnce(ctx)
			if err != nil {
				if errors.Is(err, riverdriver.ErrNotImplemented) {
					continue
				}
				if errors.Is(err, context.Canceled) || errors.Is(err, startstop.ErrStop) {
					return
				}
				s.Logger.ErrorContext(ctx, s.Name+": Error promoting sequences", slog.String("error", err.Error()))
				continue
			}
			if len(queues) > 0 {
				s.TestSignals.PromotedBatch.Signal(queues)
				s.Logger.InfoContext(ctx, s.Name+riversharedmaintenance.LogPrefixRanSuccessfully, slog.Int("num_queues_promoted", len(queues)))
			}
		}
	}()
	return nil
}

func (s *SequencePromoter) runOnce(ctx context.Context) ([]string, error) {
	res, err := s.exec.SequencePromoteFromTable(ctx, &riverdriver.SequencePromoteFromTableParams{Max: 1000, Schema: s.config.Schema})
	if err != nil {
		return nil, err
	}
	if res == nil || len(res.PromotedQueues) == 0 {
		return nil, nil
	}
	promotedQueues := make([]string, 0)
	for _, queue := range res.PromotedQueues {
		queues, err := s.exec.SequencePromote(ctx, &riverdriver.SequencePromoteParams{Max: 1000, Queue: queue, Schema: s.config.Schema})
		if err != nil {
			return nil, err
		}
		promotedQueues = append(promotedQueues, queues...)
	}
	if len(promotedQueues) == 0 {
		return nil, nil
	}
	fake := make([]*rivertype.JobInsertResult, 0, len(promotedQueues))
	payloads := make([]string, 0, len(promotedQueues))
	for _, queue := range promotedQueues {
		fake = append(fake, &rivertype.JobInsertResult{Job: &rivertype.JobRow{Queue: queue}})
		payloads = append(payloads, fmt.Sprintf("{\"queue\": %q}", queue))
	}
	s.config.NotifyNonTxJobInsert(ctx, fake)
	if err := s.exec.NotifyMany(ctx, &riverdriver.NotifyManyParams{Payload: payloads, Schema: s.config.Schema, Topic: string(notifier.NotificationTopicInsert)}); err != nil {
		return nil, err
	}
	serviceutil.CancellableSleep(ctx, randutil.DurationBetween(riversharedmaintenance.BatchBackoffMin, riversharedmaintenance.BatchBackoffMax))
	return promotedQueues, nil
}
