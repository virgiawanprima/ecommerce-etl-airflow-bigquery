.PHONY: check dag-check self-check run verify bq-verify

# Standard verification entrypoint (equivalent to a test suite for this project).
check: dag-check self-check

# Full verification: unit checks + BigQuery state.
verify: check bq-verify

# Verify DAG syntax + 5-task registration + no import errors.
dag-check:
	@echo "=== DAG import & registration ==="
	docker compose exec -T airflow-scheduler python -c "import sys; sys.path.insert(0, '/opt/airflow/dags'); import olist_etl_pipeline as m; d=m.dag; assert {t.task_id for t in d.tasks}=={'extract','validate_schema','transform','qdc_validation','load'}, 'task set mismatch'; print('DAG OK:', d.dag_id, '| tasks:', sorted(t.task_id for t in d.tasks))"
	@echo "=== import errors ==="
	docker compose exec -T airflow-scheduler airflow dags list-import-errors | tail -1
	@echo "=== pip integrity ==="
	docker compose exec -T airflow-scheduler python -m pip check

# Run the assert-based unit self-checks for QDC + PII logic.
self-check:
	@echo "=== self_check.py ==="
	docker compose exec -T airflow-scheduler python /opt/airflow/dags/utils/self_check.py

# Trigger one end-to-end DAG run (safe: idempotent full/incremental).
run:
	docker compose exec -T airflow-scheduler airflow dags trigger olist_etl_pipeline

# Verify BigQuery table counts (reads project/dataset/location from .env).
bq-verify:
	@set -a; . ./.env; set +a; \
	docker compose exec -T airflow-worker python -c "from google.cloud import bigquery; c=bigquery.Client(project='$${GCP_PROJECT_ID}', location='$${BIGQUERY_LOCATION}'); [print(f'{t.table_id:15s} {c.get_table(t.reference).num_rows:>8,} rows') for t in c.list_tables('$${BIGQUERY_DATASET}') if t.table_id.startswith(('fact_','dim_'))]"
