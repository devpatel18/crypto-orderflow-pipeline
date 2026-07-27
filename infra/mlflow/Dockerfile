FROM ghcr.io/mlflow/mlflow:v2.20.2

# The official image ships neither the Postgres driver nor S3 support
RUN pip install --no-cache-dir psycopg2-binary boto3
