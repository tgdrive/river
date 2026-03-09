ALTER TABLE river_job
    ADD COLUMN workflow_id text,
    ADD COLUMN workflow_task_name text,
    ADD COLUMN workflow_deps text[] NOT NULL DEFAULT '{}',
    ADD COLUMN sequence_key text,
    ADD COLUMN batch_key text,
    ADD COLUMN batch_id text;

CREATE TABLE river_job_dead_letter (
    id bigint PRIMARY KEY,
    job jsonb NOT NULL,
    moved_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE river_periodic_job (
    id bigserial PRIMARY KEY,
    name text NOT NULL UNIQUE,
    queue text NOT NULL,
    next_run_at timestamptz NOT NULL,
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE river_producer (
    id bigint PRIMARY KEY,
    queue_name text NOT NULL,
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE river_sequence (
    id bigserial PRIMARY KEY,
    queue text NOT NULL,
    key text NOT NULL,
    latest_job_id bigint NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE(queue, key)
);

CREATE TABLE river_workflow (
    id text PRIMARY KEY,
    name text NOT NULL DEFAULT '',
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

-- name: JobDeadLetterDeleteByID :one
DELETE FROM /* TEMPLATE: schema */river_job_dead_letter
WHERE id = @id
RETURNING job;

-- name: JobDeadLetterGetAll :many
SELECT job
FROM /* TEMPLATE: schema */river_job_dead_letter
ORDER BY moved_at DESC
LIMIT @max;

-- name: JobDeadLetterGetByID :one
SELECT job
FROM /* TEMPLATE: schema */river_job_dead_letter
WHERE id = @id;

-- name: JobDeadLetterUpsert :exec
INSERT INTO /* TEMPLATE: schema */river_job_dead_letter (id, job)
VALUES (@id, @job)
ON CONFLICT (id) DO UPDATE
SET job = EXCLUDED.job,
    moved_at = now();

-- name: JobDeleteByIDManyReturningIDs :many
DELETE FROM /* TEMPLATE: schema */river_job
WHERE id = any(@ids::bigint[])
RETURNING id;

-- name: JobGetAvailableForBatch :many
WITH candidate_jobs AS (
    SELECT id
    FROM /* TEMPLATE: schema */river_job rj
    WHERE rj.queue = @queue
      AND rj.kind = @kind
      AND rj.state = 'available'
      AND rj.scheduled_at <= coalesce(sqlc.narg('now')::timestamptz, now())
      AND coalesce(rj.batch_key, rj.metadata->>'batch_key', rj.kind) = @batch_key
    ORDER BY rj.priority ASC, rj.scheduled_at ASC, rj.id ASC
    LIMIT @max_to_lock
    FOR UPDATE SKIP LOCKED
)
UPDATE /* TEMPLATE: schema */river_job j
SET state = 'running',
    attempt = j.attempt + 1,
    attempted_at = coalesce(sqlc.narg('now')::timestamptz, now()),
    attempted_by = array_append(coalesce(j.attempted_by, '{}'), @attempted_by)
FROM candidate_jobs c
WHERE j.id = c.id
RETURNING j.id;

-- name: JobGetAvailablePartitionKeys :many
SELECT DISTINCT concat(
    CASE WHEN @partition_by_kind::boolean THEN kind ELSE '' END,
    '|',
    CASE
        WHEN @partition_by_args::text[] IS NULL THEN ''
        WHEN cardinality(@partition_by_args::text[]) = 0 THEN args::jsonb::text
        ELSE coalesce((
            SELECT jsonb_object_agg(key, args->key ORDER BY key)
            FROM unnest(@partition_by_args::text[]) AS key
            WHERE args ? key
        ), '{}'::jsonb)::text
    END
)
FROM /* TEMPLATE: schema */river_job
WHERE queue = @queue
  AND state = 'available'
  AND scheduled_at <= coalesce(sqlc.narg('now')::timestamptz, now());

CREATE OR REPLACE FUNCTION river_job_get_available_limited_ids(
    p_queue text,
    p_now timestamptz,
    p_global_limit integer,
    p_local_limit integer,
    p_max_to_lock integer,
    p_max_attempted_by integer,
    p_attempted_by text,
    p_available_partition_keys text[],
    p_current_producer_partition_keys text[],
    p_current_producer_partition_running_counts integer[],
    p_partition_by_args text[],
    p_partition_by_kind boolean
)
RETURNS TABLE(id bigint)
LANGUAGE sql
AS $$
WITH fetch_lock AS (
    SELECT pg_advisory_xact_lock(hashtext(p_queue), 0)
),
running AS (
    SELECT
        concat(
            CASE WHEN p_partition_by_kind THEN kind ELSE '' END,
            '|',
            CASE
                WHEN p_partition_by_args IS NULL THEN ''
                WHEN cardinality(p_partition_by_args) = 0 THEN args::jsonb::text
                ELSE coalesce((
                    SELECT jsonb_object_agg(key, args->key ORDER BY key)
                    FROM unnest(p_partition_by_args) AS key
                    WHERE args ? key
                ), '{}'::jsonb)::text
            END
        ) AS partition_key,
        count(*) AS global_running,
        count(*) FILTER (WHERE attempted_by[array_length(attempted_by, 1)] = p_attempted_by) AS local_running
    FROM river_job
    CROSS JOIN fetch_lock
    WHERE queue = p_queue
      AND state = 'running'
    GROUP BY 1
),
current_producer_running AS (
    SELECT
        unnest(p_current_producer_partition_keys) AS partition_key,
        unnest(p_current_producer_partition_running_counts) AS local_running
),
available_ranked AS (
    SELECT
        id,
        priority,
        scheduled_at,
        concat(
            CASE WHEN p_partition_by_kind THEN kind ELSE '' END,
            '|',
            CASE
                WHEN p_partition_by_args IS NULL THEN ''
                WHEN cardinality(p_partition_by_args) = 0 THEN args::jsonb::text
                ELSE coalesce((
                    SELECT jsonb_object_agg(key, args->key ORDER BY key)
                    FROM unnest(p_partition_by_args) AS key
                    WHERE args ? key
                ), '{}'::jsonb)::text
            END
        ) AS partition_key,
        row_number() OVER (
            PARTITION BY concat(
                CASE WHEN p_partition_by_kind THEN kind ELSE '' END,
                '|',
                CASE
                    WHEN p_partition_by_args IS NULL THEN ''
                    WHEN cardinality(p_partition_by_args) = 0 THEN args::jsonb::text
                    ELSE coalesce((
                        SELECT jsonb_object_agg(key, args->key ORDER BY key)
                        FROM unnest(p_partition_by_args) AS key
                        WHERE args ? key
                    ), '{}'::jsonb)::text
                END
            )
            ORDER BY priority ASC, scheduled_at ASC, id ASC
        ) AS partition_rank
    FROM river_job
    CROSS JOIN fetch_lock
    WHERE queue = p_queue
      AND state = 'available'
      AND scheduled_at <= p_now
      AND (p_available_partition_keys IS NULL OR concat(
            CASE WHEN p_partition_by_kind THEN kind ELSE '' END,
            '|',
            CASE
                WHEN p_partition_by_args IS NULL THEN ''
                WHEN cardinality(p_partition_by_args) = 0 THEN args::jsonb::text
                ELSE coalesce((
                    SELECT jsonb_object_agg(key, args->key ORDER BY key)
                    FROM unnest(p_partition_by_args) AS key
                    WHERE args ? key
                ), '{}'::jsonb)::text
            END
        ) = any(p_available_partition_keys))
),
eligible_ids AS (
    SELECT a.id, a.priority, a.scheduled_at
    FROM available_ranked a
    LEFT JOIN running r ON r.partition_key = a.partition_key
    LEFT JOIN current_producer_running cpr ON cpr.partition_key = a.partition_key
    WHERE a.partition_rank <= greatest(0, p_global_limit - coalesce(r.global_running, 0))
      AND a.partition_rank <= greatest(0, p_local_limit - greatest(coalesce(r.local_running, 0), coalesce(cpr.local_running, 0)))
    ORDER BY a.priority ASC, a.scheduled_at ASC, a.id ASC
    LIMIT p_max_to_lock
),
candidate_jobs AS (
    SELECT j.id
    FROM river_job j
    JOIN eligible_ids eids ON eids.id = j.id
    ORDER BY eids.priority ASC, eids.scheduled_at ASC, j.id ASC
    FOR UPDATE OF j SKIP LOCKED
)
UPDATE river_job j
SET state = 'running',
    attempt = j.attempt + 1,
    attempted_at = p_now,
    attempted_by = array_append(
        CASE
            WHEN array_length(j.attempted_by, 1) >= p_max_attempted_by
                THEN j.attempted_by[array_length(j.attempted_by, 1) + 2 - p_max_attempted_by:]
            ELSE j.attempted_by
        END,
        p_attempted_by
    )
FROM candidate_jobs c
WHERE j.id = c.id
RETURNING j.id;
$$;

-- name: JobGetAvailableLimited :many
SELECT *
FROM /* TEMPLATE: schema */river_job_get_available_limited_ids(
    @queue,
    coalesce(sqlc.narg('now')::timestamptz, now()),
    @global_limit,
    @local_limit,
    @max_to_lock,
    @max_attempted_by,
    @attempted_by,
    @available_partition_keys::text[],
    @current_producer_partition_keys::text[],
    @current_producer_partition_running_counts::integer[],
    @partition_by_args::text[],
    @partition_by_kind
);

-- name: PGTryAdvisoryXactLock :one
SELECT pg_try_advisory_xact_lock(@key::bigint);

-- name: PeriodicJobGetAllExt :many
SELECT id, created_at, updated_at, name, queue, next_run_at, metadata
FROM /* TEMPLATE: schema */river_periodic_job
ORDER BY id ASC;

-- name: PeriodicJobGetByIDExt :one
SELECT id, created_at, updated_at, name, queue, next_run_at, metadata
FROM /* TEMPLATE: schema */river_periodic_job
WHERE id = @id;

-- name: PeriodicJobInsertExt :one
INSERT INTO /* TEMPLATE: schema */river_periodic_job (name, queue, next_run_at, metadata)
VALUES (@name, @queue, @next_run_at, @metadata)
RETURNING id, created_at, updated_at, name, queue, next_run_at, metadata;

-- name: PeriodicJobDeleteStale :many
DELETE FROM /* TEMPLATE: schema */river_periodic_job
WHERE updated_at < @stale_updated_at_horizon
RETURNING id, created_at, updated_at, name, queue, next_run_at, metadata;

-- name: PeriodicJobTouchByID :exec
UPDATE /* TEMPLATE: schema */river_periodic_job
SET updated_at = coalesce(sqlc.narg('now')::timestamptz, now())
WHERE id = @id;

-- name: PeriodicJobUpsertExt :one
INSERT INTO /* TEMPLATE: schema */river_periodic_job (name, queue, next_run_at, metadata)
VALUES (@name, @queue, @next_run_at, @metadata)
ON CONFLICT (name) DO UPDATE
SET queue = EXCLUDED.queue,
    next_run_at = EXCLUDED.next_run_at,
    metadata = EXCLUDED.metadata,
    updated_at = now()
RETURNING id, created_at, updated_at, name, queue, next_run_at, metadata;

-- name: ProducerDeleteExt :exec
DELETE FROM /* TEMPLATE: schema */river_producer
WHERE id = @id;

-- name: ProducerGetByIDExt :one
SELECT id, created_at, updated_at, queue_name, metadata
FROM /* TEMPLATE: schema */river_producer
WHERE id = @id;

-- name: ProducerInsertOrUpdateExt :one
INSERT INTO /* TEMPLATE: schema */river_producer (id, queue_name, metadata, created_at, updated_at)
VALUES (@id, @queue_name, @metadata, coalesce(sqlc.narg('now')::timestamptz, now()), coalesce(sqlc.narg('now')::timestamptz, now()))
ON CONFLICT (id) DO UPDATE
SET queue_name = EXCLUDED.queue_name,
    metadata = EXCLUDED.metadata,
    updated_at = EXCLUDED.updated_at
RETURNING id, created_at, updated_at, queue_name, metadata;

-- name: ProducerDeleteStaleExcludingID :exec
DELETE FROM /* TEMPLATE: schema */river_producer
WHERE updated_at < @stale_updated_at_horizon
  AND id <> @id;

-- name: ProducerKeepAliveUpsert :one
INSERT INTO /* TEMPLATE: schema */river_producer (id, queue_name, metadata, created_at, updated_at)
VALUES (@id, @queue_name, '{}'::jsonb, now(), now())
ON CONFLICT (id) DO UPDATE
SET queue_name = EXCLUDED.queue_name,
    updated_at = now()
RETURNING id, created_at, updated_at, queue_name, metadata;

-- name: ProducerListByQueueExt :many
SELECT id, queue_name, metadata, updated_at
FROM /* TEMPLATE: schema */river_producer
WHERE queue_name = @queue_name
ORDER BY id ASC;

-- name: ProducerUpdateExt :one
UPDATE /* TEMPLATE: schema */river_producer
SET metadata = @metadata,
    updated_at = coalesce(sqlc.narg('updated_at')::timestamptz, now())
WHERE id = @id
RETURNING id, created_at, updated_at, queue_name, metadata;

-- name: QueueGetMetadataForInsert :many
SELECT name, metadata
FROM /* TEMPLATE: schema */river_queue
WHERE name = any(@queue_names::text[]);

-- name: SequenceAppend :exec
INSERT INTO /* TEMPLATE: schema */river_sequence (queue, key, latest_job_id, created_at, updated_at)
VALUES (@queue, @key, @latest_job_id, now(), now())
ON CONFLICT (queue, key) DO UPDATE
SET latest_job_id = greatest(/* TEMPLATE: schema */river_sequence.latest_job_id, EXCLUDED.latest_job_id),
    updated_at = now();

-- name: SequenceListExt :many
SELECT id, created_at, updated_at, queue, key, latest_job_id
FROM /* TEMPLATE: schema */river_sequence
WHERE queue = @queue
ORDER BY updated_at DESC
LIMIT @max;

-- name: SequencePromote :many
WITH promotable AS (
    SELECT j.id, j.queue
    FROM /* TEMPLATE: schema */river_job j
    WHERE j.queue = @queue
      AND j.state = 'pending'
      AND coalesce(j.sequence_key, j.metadata->>'sequence_key') IS NOT NULL
      AND NOT EXISTS (
        SELECT 1
        FROM /* TEMPLATE: schema */river_job prev
        WHERE prev.queue = j.queue
          AND coalesce(prev.sequence_key, prev.metadata->>'sequence_key') = coalesce(j.sequence_key, j.metadata->>'sequence_key')
          AND prev.id < j.id
          AND (
            prev.state IN ('available', 'pending', 'retryable', 'running', 'scheduled')
            OR (prev.state = 'cancelled' AND NOT coalesce((j.metadata->>'sequence_continue_on_cancelled')::boolean, false))
            OR (prev.state = 'discarded' AND NOT coalesce((j.metadata->>'sequence_continue_on_discarded')::boolean, false))
          )
      )
      AND NOT EXISTS (
        SELECT 1
        FROM /* TEMPLATE: schema */river_job same_seq_pending
        WHERE same_seq_pending.queue = j.queue
          AND same_seq_pending.state = 'pending'
          AND coalesce(same_seq_pending.sequence_key, same_seq_pending.metadata->>'sequence_key') = coalesce(j.sequence_key, j.metadata->>'sequence_key')
          AND same_seq_pending.id < j.id
      )
    ORDER BY j.id ASC
    LIMIT @max
)
UPDATE /* TEMPLATE: schema */river_job j
SET state = 'available',
    scheduled_at = now()
FROM promotable
WHERE j.id = promotable.id
RETURNING j.queue;

-- name: SequencePromoteFromTable :many
SELECT DISTINCT queue
FROM /* TEMPLATE: schema */river_sequence
ORDER BY queue ASC
LIMIT @max;

-- name: WorkflowCancelByWorkflowID :many
UPDATE /* TEMPLATE: schema */river_job
SET state = 'cancelled',
    finalized_at = now()
WHERE coalesce(workflow_id, metadata->>'workflow_id') = @workflow_id
  AND state IN ('available', 'pending', 'retryable', 'scheduled')
RETURNING id;

-- name: WorkflowCancelByIDs :many
UPDATE /* TEMPLATE: schema */river_job
SET state = 'cancelled',
    finalized_at = now()
WHERE id = any(@ids::bigint[])
  AND state IN ('available', 'pending', 'retryable', 'scheduled')
RETURNING id;

-- name: WorkflowCancelWithFailedDepsByIDs :many
UPDATE /* TEMPLATE: schema */river_job
SET state = 'cancelled',
    finalized_at = coalesce(sqlc.narg('workflow_deps_failed_at')::timestamptz, now()),
    metadata = metadata || jsonb_build_object(
        'workflow_deps_failed_at', to_jsonb(coalesce(sqlc.narg('workflow_deps_failed_at')::timestamptz, now()))
    )
WHERE id = any(@ids::bigint[])
  AND state IN ('available', 'pending', 'retryable', 'scheduled')
RETURNING id;

-- name: WorkflowGetPendingIDs :many
SELECT id, coalesce(workflow_task_name, metadata->>'workflow_task_name', '') AS task
FROM /* TEMPLATE: schema */river_job
WHERE coalesce(workflow_id, metadata->>'workflow_id') = @workflow_id
  AND state = 'pending'
ORDER BY id ASC;

-- name: WorkflowJobGetByTaskNameID :one
SELECT id
FROM /* TEMPLATE: schema */river_job
WHERE coalesce(workflow_id, metadata->>'workflow_id') = @workflow_id
  AND coalesce(workflow_task_name, metadata->>'workflow_task_name') = @task_name
ORDER BY id DESC
LIMIT 1;

-- name: WorkflowJobListIDs :many
SELECT id
FROM /* TEMPLATE: schema */river_job
WHERE coalesce(workflow_id, metadata->>'workflow_id') = @workflow_id
ORDER BY id ASC;

-- name: WorkflowListActive :many
SELECT workflow_id, min(created_at) AS created_at, max(created_at) AS updated_at
FROM (
    SELECT coalesce(workflow_id, metadata->>'workflow_id') AS workflow_id, created_at, state
    FROM /* TEMPLATE: schema */river_job
    WHERE coalesce(workflow_id, metadata->>'workflow_id') IS NOT NULL
) grouped
GROUP BY workflow_id
HAVING bool_or(state NOT IN ('cancelled','completed','discarded'))
ORDER BY max(created_at) DESC
LIMIT @max;

-- name: WorkflowListAll :many
SELECT workflow_id, min(created_at) AS created_at, max(created_at) AS updated_at
FROM (
    SELECT coalesce(workflow_id, metadata->>'workflow_id') AS workflow_id, created_at, state
    FROM /* TEMPLATE: schema */river_job
    WHERE coalesce(workflow_id, metadata->>'workflow_id') IS NOT NULL
) grouped
GROUP BY workflow_id
ORDER BY max(created_at) DESC
LIMIT @max;

-- name: WorkflowListInactive :many
SELECT workflow_id, min(created_at) AS created_at, max(created_at) AS updated_at
FROM (
    SELECT coalesce(workflow_id, metadata->>'workflow_id') AS workflow_id, created_at, state
    FROM /* TEMPLATE: schema */river_job
    WHERE coalesce(workflow_id, metadata->>'workflow_id') IS NOT NULL
) grouped
GROUP BY workflow_id
HAVING NOT bool_or(state NOT IN ('cancelled','completed','discarded'))
ORDER BY max(created_at) DESC
LIMIT @max;

-- name: WorkflowLoadDepTasksAndIDs :many
SELECT coalesce(workflow_task_name, metadata->>'workflow_task_name') AS task, id
FROM /* TEMPLATE: schema */river_job
WHERE coalesce(workflow_id, metadata->>'workflow_id') = @workflow_id
  AND coalesce(workflow_task_name, metadata->>'workflow_task_name') = any(@task_names::text[]);

-- name: WorkflowLoadJobsWithDeps :many
SELECT id,
       coalesce(workflow_task_name, metadata->>'workflow_task_name', '') AS task,
       coalesce(nullif(workflow_deps, '{}'::text[])::text, (metadata->'workflow_deps')::text, '{}') AS deps_raw
FROM /* TEMPLATE: schema */river_job
WHERE coalesce(workflow_id, metadata->>'workflow_id') = @workflow_id;

-- name: WorkflowLoadTaskDepsByJobID :one
SELECT coalesce(nullif(workflow_deps, '{}'::text[])::text, (metadata->'workflow_deps')::text, '{}')
FROM /* TEMPLATE: schema */river_job
WHERE id = @id;

-- name: WorkflowLoadTasksByNames :many
SELECT coalesce(workflow_task_name, metadata->>'workflow_task_name','') AS task,
       coalesce(nullif(workflow_deps, '{}'::text[])::text, (metadata->'workflow_deps')::text, '{}') AS deps_raw
FROM /* TEMPLATE: schema */river_job
WHERE coalesce(workflow_id, metadata->>'workflow_id') = @workflow_id
  AND coalesce(workflow_task_name, metadata->>'workflow_task_name') = any(@task_names::text[]);

-- name: WorkflowRetryByIDMany :many
UPDATE /* TEMPLATE: schema */river_job
SET state = 'available',
    attempt = CASE WHEN @reset_history::boolean THEN 0 ELSE attempt END,
    attempted_at = CASE WHEN @reset_history::boolean THEN NULL ELSE attempted_at END,
    attempted_by = CASE WHEN @reset_history::boolean THEN NULL ELSE attempted_by END,
    errors = CASE WHEN @reset_history::boolean THEN '{}'::jsonb[] ELSE errors END,
    finalized_at = NULL,
    max_attempts = CASE WHEN @reset_history::boolean THEN max_attempts WHEN attempt = max_attempts THEN max_attempts + 1 ELSE max_attempts END,
    metadata = CASE WHEN @reset_history::boolean THEN metadata - 'output' ELSE metadata END,
    scheduled_at = coalesce(sqlc.narg('now')::timestamptz, now())
WHERE id = any(@ids::bigint[])
RETURNING id;

-- name: WorkflowSetPendingByIDMany :many
UPDATE /* TEMPLATE: schema */river_job
SET state = 'pending'
WHERE id = any(@ids::bigint[])
  AND state = 'available'
RETURNING id;

-- name: WorkflowCountRunning :one
SELECT count(*)
FROM /* TEMPLATE: schema */river_job
WHERE coalesce(workflow_id, metadata->>'workflow_id') = @workflow_id
  AND state = 'running';

-- name: WorkflowStageJobsByIDMany :many
UPDATE /* TEMPLATE: schema */river_job
SET state = 'available',
    scheduled_at = now()
WHERE id = any(@ids::bigint[])
  AND state = 'pending'
RETURNING id;
