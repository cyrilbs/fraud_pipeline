FROM python:3.11-slim

WORKDIR /usr/app

RUN pip install --no-cache-dir "dbt-core>=1.8,<2" "dbt-duckdb>=1.8,<2"

COPY . .

ENTRYPOINT ["dbt"]
