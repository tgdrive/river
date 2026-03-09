package river

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/internal/maintenance"
	"github.com/riverqueue/river/rivershared/riversharedmaintenance"
)

func TestConfig_WithDefaults(t *testing.T) {
	t.Parallel()

	defaults := (&Config{DurablePeriodicJobs: &DurablePeriodicJobsConfig{Enabled: true}}).WithDefaults()
	require.NotNil(t, defaults)
	require.NotNil(t, defaults.Logger)
	require.NotNil(t, defaults.RetryPolicy)
	require.NotEmpty(t, defaults.ID)
	require.Equal(t, FetchCooldownDefault, defaults.FetchCooldown)
	require.Equal(t, FetchPollIntervalDefault, defaults.FetchPollInterval)
	require.Equal(t, JobTimeoutDefault, defaults.JobTimeout)
	require.Equal(t, MaxAttemptsDefault, defaults.MaxAttempts)
	require.Equal(t, PartitionKeyCacheTTLDefault, defaults.PartitionKeyCacheTTL)
	require.NotNil(t, defaults.DurablePeriodicJobs)
	require.NotNil(t, defaults.DurablePeriodicJobs.NextRunAtRatchetFunc)
	require.Equal(t, durablePeriodicJobsStaleThresholdDefault, defaults.DurablePeriodicJobs.StaleThreshold)
	require.Equal(t, durablePeriodicJobsStartStaggerSpreadDefault, defaults.DurablePeriodicJobs.StartStaggerSpread)
	require.Equal(t, durablePeriodicJobsStartStaggerThresholdDefault, defaults.DurablePeriodicJobs.StartStaggerThreshold)
	require.Equal(t, maintenance.JobRescuerRescueAfterDefault, defaults.RescueStuckJobsAfter)
	require.Equal(t, riversharedmaintenance.CancelledJobRetentionPeriodDefault, defaults.CancelledJobRetentionPeriod)
	require.Equal(t, riversharedmaintenance.CompletedJobRetentionPeriodDefault, defaults.CompletedJobRetentionPeriod)
	require.Equal(t, riversharedmaintenance.DiscardedJobRetentionPeriodDefault, defaults.DiscardedJobRetentionPeriod)
}

func TestConfig_WithDefaults_RescueAfterFollowsJobTimeout(t *testing.T) {
	t.Parallel()

	config := (&Config{JobTimeout: 5 * time.Minute}).WithDefaults()
	require.Equal(t, 5*time.Minute+maintenance.JobRescuerRescueAfterDefault, config.RescueStuckJobsAfter)

	config = (&Config{JobTimeout: 5 * time.Minute, RescueStuckJobsAfter: 10 * time.Minute}).WithDefaults()
	require.Equal(t, 10*time.Minute, config.RescueStuckJobsAfter)
	config = (&Config{RetryPolicy: &DefaultClientRetryPolicy{}}).WithDefaults()
	require.NotNil(t, config.RetryPolicy)

	config = (&Config{PartitionKeyCacheTTL: -1}).WithDefaults()
	require.Equal(t, time.Duration(-1), config.PartitionKeyCacheTTL)

	ratcheted := (&Config{DurablePeriodicJobs: &DurablePeriodicJobsConfig{Enabled: true}}).WithDefaults().DurablePeriodicJobs
	now := time.Now().UTC()
	require.Equal(t, now.Add(-time.Minute), ratcheted.NextRunAtRatchetFunc(now.Add(-time.Minute), now))
}
