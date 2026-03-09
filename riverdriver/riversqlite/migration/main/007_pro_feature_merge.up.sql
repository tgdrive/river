ALTER TABLE /* TEMPLATE: schema */river_job ADD COLUMN workflow_id text;
ALTER TABLE /* TEMPLATE: schema */river_job ADD COLUMN workflow_task_name text;
ALTER TABLE /* TEMPLATE: schema */river_job ADD COLUMN workflow_deps blob NOT NULL DEFAULT '[]';
ALTER TABLE /* TEMPLATE: schema */river_job ADD COLUMN sequence_key text;
ALTER TABLE /* TEMPLATE: schema */river_job ADD COLUMN batch_key text;
ALTER TABLE /* TEMPLATE: schema */river_job ADD COLUMN batch_id text;

CREATE TABLE /* TEMPLATE: schema */river_job_dead_letter (
    id integer PRIMARY KEY,
    job blob NOT NULL,
    moved_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE /* TEMPLATE: schema */river_periodic_job (
    id integer PRIMARY KEY,
    name text NOT NULL UNIQUE,
    queue text NOT NULL,
    next_run_at timestamp NOT NULL,
    metadata blob NOT NULL DEFAULT (json('{}')),
    created_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE /* TEMPLATE: schema */river_producer (
    id integer PRIMARY KEY,
    queue_name text NOT NULL,
    metadata blob NOT NULL DEFAULT (json('{}')),
    created_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE /* TEMPLATE: schema */river_sequence (
    id integer PRIMARY KEY,
    queue text NOT NULL,
    key text NOT NULL,
    latest_job_id integer NOT NULL,
    created_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(queue, key)
);

CREATE TABLE /* TEMPLATE: schema */river_workflow (
    id text PRIMARY KEY,
    name text NOT NULL DEFAULT '',
    metadata blob NOT NULL DEFAULT (json('{}')),
    created_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP
);
