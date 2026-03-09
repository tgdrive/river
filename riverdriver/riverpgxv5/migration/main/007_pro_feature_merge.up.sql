CREATE TABLE IF NOT EXISTS /* TEMPLATE: schema */river_job_dead_letter (
    id bigint PRIMARY KEY,
    job jsonb NOT NULL,
    moved_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS /* TEMPLATE: schema */river_periodic_job (
    id bigserial PRIMARY KEY,
    name text NOT NULL UNIQUE,
    queue text NOT NULL,
    next_run_at timestamptz NOT NULL,
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS /* TEMPLATE: schema */river_producer (
    id bigint PRIMARY KEY,
    queue_name text NOT NULL,
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS river_producer_queue_name_idx ON /* TEMPLATE: schema */river_producer (queue_name);

CREATE TABLE IF NOT EXISTS /* TEMPLATE: schema */river_sequence (
    id bigserial PRIMARY KEY,
    queue text NOT NULL,
    key text NOT NULL,
    latest_job_id bigint NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE(queue, key)
);

CREATE TABLE IF NOT EXISTS /* TEMPLATE: schema */river_workflow (
    id text PRIMARY KEY,
    name text NOT NULL DEFAULT '',
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

ALTER TABLE /* TEMPLATE: schema */river_job
    ADD COLUMN IF NOT EXISTS workflow_id text,
    ADD COLUMN IF NOT EXISTS workflow_task_name text,
    ADD COLUMN IF NOT EXISTS workflow_deps text[] NOT NULL DEFAULT '{}',
    ADD COLUMN IF NOT EXISTS sequence_key text,
    ADD COLUMN IF NOT EXISTS batch_key text,
    ADD COLUMN IF NOT EXISTS batch_id text;

CREATE INDEX IF NOT EXISTS river_job_workflow_id_idx ON /* TEMPLATE: schema */river_job (workflow_id);
CREATE INDEX IF NOT EXISTS river_job_workflow_task_name_idx ON /* TEMPLATE: schema */river_job (workflow_task_name);
CREATE INDEX IF NOT EXISTS river_job_sequence_key_idx ON /* TEMPLATE: schema */river_job (sequence_key);

CREATE OR REPLACE FUNCTION /* TEMPLATE: schema */river_job_get_available_limited_ids(
    p_queue text,
    p_now timestamptz,
    p_global_limit integer,
    p_local_limit integer,
    p_max_to_lock integer,
    p_max_attempted_by integer,
    p_attempted_by text,
    p_partition_by_kind boolean
)
RETURNS TABLE(id bigint)
LANGUAGE sql
AS $$
WITH running AS (
    SELECT
        CASE WHEN p_partition_by_kind THEN kind ELSE '' END AS partition_key,
        count(*) AS global_running,
        count(*) FILTER (WHERE attempted_by[array_length(attempted_by, 1)] = p_attempted_by) AS local_running
    FROM /* TEMPLATE: schema */river_job
    WHERE queue = p_queue
      AND state = 'running'
    GROUP BY 1
),
available_ranked AS (
    SELECT
        id,
        priority,
        scheduled_at,
        CASE WHEN p_partition_by_kind THEN kind ELSE '' END AS partition_key,
        row_number() OVER (
            PARTITION BY CASE WHEN p_partition_by_kind THEN kind ELSE '' END
            ORDER BY priority ASC, scheduled_at ASC, id ASC
        ) AS partition_rank
    FROM /* TEMPLATE: schema */river_job
    WHERE queue = p_queue
      AND state = 'available'
      AND scheduled_at <= p_now
),
eligible_ids AS (
    SELECT a.id, a.priority, a.scheduled_at
    FROM available_ranked a
    LEFT JOIN running r ON r.partition_key = a.partition_key
    WHERE a.partition_rank <= greatest(0, p_global_limit - coalesce(r.global_running, 0))
      AND a.partition_rank <= greatest(0, p_local_limit - coalesce(r.local_running, 0))
    ORDER BY a.priority ASC, a.scheduled_at ASC, a.id ASC
    LIMIT p_max_to_lock
),
candidate_jobs AS (
    SELECT j.id
    FROM /* TEMPLATE: schema */river_job j
    JOIN eligible_ids eids ON eids.id = j.id
    ORDER BY eids.priority ASC, eids.scheduled_at ASC, j.id ASC
    FOR UPDATE OF j SKIP LOCKED
)
UPDATE /* TEMPLATE: schema */river_job j
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
