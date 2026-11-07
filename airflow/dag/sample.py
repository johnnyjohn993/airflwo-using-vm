exitfrom datetime import datetime, timedelta
import io
import os
import pandas as pd
import pymssql
import boto3
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# ==== Azure SQL Database credentials ====
# Read from environment. The values are injected by docker-compose from the
# git-ignored .env file — never hardcode them here.
AZURE_SQL_SERVER = os.environ.get('AZURE_SQL_SERVER', '')
AZURE_SQL_PORT = int(os.environ.get('AZURE_SQL_PORT', '1433'))
AZURE_SQL_DATABASE = os.environ.get('AZURE_SQL_DATABASE', '')
AZURE_SQL_USER = os.environ.get('AZURE_SQL_USER', '')
AZURE_SQL_PASSWORD = os.environ.get('AZURE_SQL_PASSWORD', '')

# ==== MinIO (local S3-compatible storage) credentials ====
MINIO_ENDPOINT = os.environ.get('MINIO_ENDPOINT', 'http://minio:9000')
MINIO_ACCESS_KEY = os.environ.get('MINIO_ACCESS_KEY', '')
MINIO_SECRET_KEY = os.environ.get('MINIO_SECRET_KEY', '')


def connect_azure_sql():
    """Open a connection to Azure SQL Database."""
    missing = [name for name, value in (
        ('AZURE_SQL_SERVER', AZURE_SQL_SERVER),
        ('AZURE_SQL_DATABASE', AZURE_SQL_DATABASE),
        ('AZURE_SQL_USER', AZURE_SQL_USER),
        ('AZURE_SQL_PASSWORD', AZURE_SQL_PASSWORD),
    ) if not value]
    if missing:
        raise ValueError(
            f"Missing Azure SQL credentials in environment: {', '.join(missing)}. "
            "Set them in the git-ignored .env file (see .env.example)."
        )
    return pymssql.connect(
        server=AZURE_SQL_SERVER,
        port=AZURE_SQL_PORT,
        user=AZURE_SQL_USER,
        password=AZURE_SQL_PASSWORD,
        database=AZURE_SQL_DATABASE,
    )


def extract_sql_to_parquet_minio():
    # 1. Extract from Azure SQL
    conn = connect_azure_sql()
    cursor = conn.cursor()
    cursor.execute("SELECT id, name, email, created_at FROM dbo.users;")
    columns = [col[0] for col in cursor.description]
    rows = cursor.fetchall()
    cursor.close()
    conn.close()

    df = pd.DataFrame.from_records(rows, columns=columns)

    if df.empty:
        print("No data found in Azure SQL 'users' table. Exiting task.")
        return

    load_date = datetime.now().strftime('%Y-%m-%d')

    # 2. Convert DataFrame to Parquet format in memory
    parquet_buffer = io.BytesIO()
    df.to_parquet(parquet_buffer, index=False, engine='pyarrow')
    parquet_buffer.seek(0)

    # 3. Connect to local MinIO (acting as S3)
    s3_client = boto3.client(
        's3',
        endpoint_url=MINIO_ENDPOINT,  # Uses the service name from docker-compose
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY
    )

    # 4. Ensure the bucket exists, then upload file
    bucket_name = 'warehouse'
    try:
        s3_client.create_bucket(Bucket=bucket_name)
    except s3_client.exceptions.BucketAlreadyOwnedByYou:
        pass  # Bucket already exists

    filename = f"users_olap/load_date={load_date}/users.parquet"

    s3_client.put_object(
        Bucket=bucket_name,
        Key=filename,
        Body=parquet_buffer.getvalue()
    )
    print(f"Successfully uploaded Parquet data to MinIO at s3://{bucket_name}/{filename}")

    # 5. Load into the Azure SQL warehouse table (dbo.users_olap)
    warehouse_conn = connect_azure_sql()
    warehouse_cursor = warehouse_conn.cursor()

    warehouse_cursor.execute("""
        IF OBJECT_ID('dbo.users_olap', 'U') IS NULL
        CREATE TABLE dbo.users_olap (
            id INT,
            name NVARCHAR(255),
            email NVARCHAR(255),
            created_at DATETIME2,
            load_date DATE
        );
    """)

    # Delete same-day rows first so DAG re-runs stay idempotent
    warehouse_cursor.execute(
        "DELETE FROM dbo.users_olap WHERE load_date = %s;",
        (load_date,)
    )

    records = list(df.itertuples(index=False, name=None))
    warehouse_cursor.executemany(
        "INSERT INTO dbo.users_olap (id, name, email, created_at, load_date) "
        "VALUES (%s, %s, %s, %s, %s);",
        [(row[0], row[1], row[2], row[3], load_date) for row in records]
    )

    warehouse_conn.commit()
    warehouse_cursor.close()
    warehouse_conn.close()
    print(f"Loaded {len(records)} rows into Azure SQL 'dbo.users_olap' for load_date={load_date}")

# Define the Airflow DAG
with DAG(
    'azure_sql_to_s3_parquet_pipeline',
    default_args=default_args,
    description='Extracts sign-up data from Azure SQL, converts to Parquet, saves to MinIO, and loads into an Azure SQL warehouse table',
    schedule_interval='@daily',
    catchup=False,
) as dag:

    upload_task = PythonOperator(
        task_id='extract_upload_and_load',
        python_callable=extract_sql_to_parquet_minio,
    )

    upload_task
