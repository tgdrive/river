DROP INDEX IF EXISTS river_job_sequence_key_idx;
DROP INDEX IF EXISTS river_job_workflow_task_name_idx;
DROP INDEX IF EXISTS river_job_workflow_id_idx;

DROP FUNCTION IF EXISTS /* TEMPLATE: schema */river_job_get_available_limited_ids(text, timestamptz, integer, integer, integer, integer, text, text[], text[], boolean);

ALTER TABLE /* TEMPLATE: schema */river_job
    DROP COLUMN IF EXISTS batch_id,
    DROP COLUMN IF EXISTS batch_key,
    DROP COLUMN IF EXISTS sequence_key,
    DROP COLUMN IF EXISTS workflow_deps,
    DROP COLUMN IF EXISTS workflow_task_name,
    DROP COLUMN IF EXISTS workflow_id;

DROP TABLE IF EXISTS /* TEMPLATE: schema */river_workflow;
DROP TABLE IF EXISTS /* TEMPLATE: schema */river_sequence;
DROP INDEX IF EXISTS river_producer_queue_name_idx;
DROP TABLE IF EXISTS /* TEMPLATE: schema */river_producer;
DROP TABLE IF EXISTS /* TEMPLATE: schema */river_periodic_job;
DROP TABLE IF EXISTS /* TEMPLATE: schema */river_job_dead_letter;
