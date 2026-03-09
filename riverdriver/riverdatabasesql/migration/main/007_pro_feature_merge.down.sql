DROP TABLE IF EXISTS /* TEMPLATE: schema */river_workflow;
DROP TABLE IF EXISTS /* TEMPLATE: schema */river_sequence;
DROP TABLE IF EXISTS /* TEMPLATE: schema */river_producer;
DROP TABLE IF EXISTS /* TEMPLATE: schema */river_periodic_job;
DROP TABLE IF EXISTS /* TEMPLATE: schema */river_job_dead_letter;

ALTER TABLE /* TEMPLATE: schema */river_job
    DROP COLUMN IF EXISTS batch_id,
    DROP COLUMN IF EXISTS batch_key,
    DROP COLUMN IF EXISTS sequence_key,
    DROP COLUMN IF EXISTS workflow_deps,
    DROP COLUMN IF EXISTS workflow_task_name,
    DROP COLUMN IF EXISTS workflow_id;
