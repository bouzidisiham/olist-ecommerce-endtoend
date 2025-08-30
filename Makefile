.PHONY: up down etl dbt dbt-test app kaggle ci docs
up: ; docker compose up -d
down: ; docker compose down
etl: ; python -m etl.load_to_postgres
dbt: ; cd dbt && dbt run --profiles-dir profiles
dbt-test: ; cd dbt && dbt test --profiles-dir profiles
kaggle: ; python etl/download_kaggle.py
app: ; streamlit run app/streamlit_app.py
ci: ; docker compose run --rm airflow bash -lc "cd /opt/airflow/dbt && dbt build --profiles-dir profiles"
docs: ; cd dbt && dbt docs generate
