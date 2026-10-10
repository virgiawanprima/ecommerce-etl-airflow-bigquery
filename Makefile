.PHONY: check dag-check self-check run verify bq-verify baseline run-baseline

# Titik masuk verifikasi standar (setara test suite untuk proyek ini).
check: dag-check self-check

# Verifikasi lengkap: pemeriksaan unit + status BigQuery.
verify: check bq-verify

# Verifikasi sintaks DAG + registrasi 5 task + tidak ada error impor.
dag-check:
	@echo "=== Impor & registrasi DAG ==="
	docker compose exec -T airflow-scheduler python -c "import sys; sys.path.insert(0, '/opt/airflow/dags'); import olist_etl_pipeline as m; d=m.dag; assert {t.task_id for t in d.tasks}=={'extract','validate_schema','transform','qdc_validation','load'}, 'kumpulan task tidak cocok'; print('DAG OK:', d.dag_id, '| tasks:', sorted(t.task_id for t in d.tasks))"
	@echo "=== error impor ==="
	docker compose exec -T airflow-scheduler airflow dags list-import-errors | tail -1
	@echo "=== integritas pip ==="
	docker compose exec -T airflow-scheduler python -m pip check

# Menjalankan unit self-check berbasis assert untuk logika QDC + PII.
self-check:
	@echo "=== self_check.py ==="
	docker compose exec -T airflow-scheduler python /opt/airflow/dags/utils/self_check.py

# Memicu satu eksekusi DAG end-to-end (aman: idempoten untuk full/incremental).
run:
	docker compose exec -T airflow-scheduler airflow dags trigger olist_etl_pipeline

# Memicu DAG baseline tanpa gerbang QDC (pembanding durasi).
run-baseline:
	docker compose exec -T airflow-scheduler airflow dags trigger olist_etl_baseline_noqdc

# Memverifikasi jumlah baris tabel BigQuery (baca project/dataset/location dari .env).
bq-verify:
	@set -a; . ./.env; set +a; \
	docker compose exec -T airflow-worker python -c "from google.cloud import bigquery; c=bigquery.Client(project='$${GCP_PROJECT_ID}', location='$${BIGQUERY_LOCATION}'); [print(f'{t.table_id:15s} {c.get_table(t.reference).num_rows:>8,} baris') for t in c.list_tables('$${BIGQUERY_DATASET}') if t.table_id.startswith(('fact_','dim_'))]"