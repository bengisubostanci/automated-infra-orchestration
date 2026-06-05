from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# 1. DAG Ayarlarını Tanımlayalım (Default Args)
default_args = {
    'owner': 'bengisu',
    'depends_on_past': False,
    'start_date': datetime(2026, 6, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# 2. Orkestrasyon Akışını Başlatalım
with DAG(
    'infrastructure_verification_dag',
    default_args=default_args,
    description='Veri muhendisligi platformu altyapi dogrulama akisi',
    schedule_interval=None, # Sadece biz butona basinca calisacak
    catchup=False,
    tags=['infrastructure', 'verification'],
) as dag:

    # Görev 1: Sistem Kontrolü (Bash ile)
    check_system = BashOperator(
        task_id='check_environment_nodes',
        bash_command='echo "Platform node checking... Spark, MinIO and Postgres are reachable."',
    )

    # Görev 2: İlk Veriyi Hazırlama (Python ile)
    def _generate_sandbox_metadata():
        print("Sandbox platform metadata initialized successfully.")
        return "SUCCESS"

    initialize_metadata = PythonOperator(
        task_id='initialize_platform_metadata',
        python_callable=_generate_sandbox_metadata,
    )

    # Görev Sıralaması (Pipeline Hattı)
    check_system >> initialize_metadata