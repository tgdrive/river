ALTER TABLE /* TEMPLATE: schema */river_job
    ADD COLUMN workflow_id text,
    ADD COLUMN workflow_task_name text,
    ADD COLUMN workflow_deps text[] NOT NULL DEFAULT '{}',
    ADD COLUMN sequence_key text,
    ADD COLUMN batch_key text,
    ADD COLUMN batch_id text;

CREATE TABLE /* TEMPLATE: schema */river_job_dead_letter (
    id bigint PRIMARY KEY,
    job jsonb NOT NULL,
    moved_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE /* TEMPLATE: schema */river_periodic_job (
    id bigserial PRIMARY KEY,
    name text NOT NULL UNIQUE,
    queue text NOT NULL,
    next_run_at timestamptz NOT NULL,
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE /* TEMPLATE: schema */river_producer (
    id bigint PRIMARY KEY,
    queue_name text NOT NULL,
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE /* TEMPLATE: schema */river_sequence (
    id bigserial PRIMARY KEY,
    queue text NOT NULL,
    key text NOT NULL,
    latest_job_id bigint NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE(queue, key)
);

CREATE TABLE /* TEMPLATE: schema */river_workflow (
    id text PRIMARY KEY,
    name text NOT NULL DEFAULT '',
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);
