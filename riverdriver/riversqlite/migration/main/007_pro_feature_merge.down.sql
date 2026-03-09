DROP TABLE IF EXISTS /* TEMPLATE: schema */river_workflow;
DROP TABLE IF EXISTS /* TEMPLATE: schema */river_sequence;
DROP TABLE IF EXISTS /* TEMPLATE: schema */river_producer;
DROP TABLE IF EXISTS /* TEMPLATE: schema */river_periodic_job;
DROP TABLE IF EXISTS /* TEMPLATE: schema */river_job_dead_letter;

ALTER TABLE /* TEMPLATE: schema */river_job DROP COLUMN batch_id;
ALTER TABLE /* TEMPLATE: schema */river_job DROP COLUMN batch_key;
ALTER TABLE /* TEMPLATE: schema */river_job DROP COLUMN sequence_key;
ALTER TABLE /* TEMPLATE: schema */river_job DROP COLUMN workflow_deps;
ALTER TABLE /* TEMPLATE: schema */river_job DROP COLUMN workflow_task_name;
ALTER TABLE /* TEMPLATE: schema */river_job DROP COLUMN workflow_id;
