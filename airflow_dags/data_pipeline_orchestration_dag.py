import os
import json
import random
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from psycopg2 import connect, sql

# --- ÇEVRESEL AYARLAR VE SABİTLER ---
MINIO_LOCAL_PATH = "/opt/airflow/dags/temp_raw_transactions.json"
POSTGRES_CONN_STR = "host=metadata-db port=5432 dbname=metadata_store user=postgres_admin password=SecurePostgresPassword456"

default_args = {
    'owner': 'bengisu',
    'depends_on_past': False,
    'start_date': datetime(2026, 6, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

# --- ADIM 1: GÖREV FONKSİYONLARI (AIRFLOW ETABI) ---

def _generate_and_upload_to_minio():
    """ Sahte finansal işlemler üretir ve veri gölüne (MinIO) simüle ederek yazar """
    print("Simulating transaction metrics generation...")
    
    # Kurumsal yapıda test verisi üretimi
    channels = ["Mobile", "Web", "ATM", "Branch"]
    statuses = ["SUCCESS", "FAILED", "PENDING"]
    
    mock_data = []
    for i in range(1, 101): # 100 adet finansal işlem satırı
        mock_data.append({
            "transaction_id": f"TXN_{random.randint(100000, 999999)}",
            "customer_id": f"CUST_{random.randint(1000, 9999)}",
            "amount": round(random.uniform(10.0, 5000.0), 2),
            "currency": "TRY",
            "channel": random.choice(channels),
            "status": random.choice(statuses),
            "timestamp": (datetime.now() - timedelta(minutes=random.randint(1, 1440))).strftime("%Y-%m-%d %H:%M:%S")
        })
        
    # Docker içerisinde paylaşımlı volüme yazarak nesne depolama katmanını simüle ediyoruz
    with open(MINIO_LOCAL_PATH, "w") as f:
        json.dump(mock_data, f, indent=4)
        
    print(f"Successfully generated 100 transactions and synchronized with container object storage.")

def _spark_transform_and_load():
    """ 
    Normalde ayrı bir spark-submit scripti olarak çalıştırılan yapıyı, 
    sandbox ortamında kaynaktan tasarruf etmek için Python altından tetikliyoruz.
    MinIO'daki JSON veriyi okur, şüpheli yüksek harcamaları filtreler ve Postgres'e yazar.
    """
    print("Initiating distributed Spark session wrapper...")
    
    # Ağ kısıtlamalarını aşmak ve hafif çalışmak için veriyi okuyup dönüştürüyoruz
    with open(MINIO_LOCAL_PATH, "r") as f:
        data = json.load(f)
        
    print(f"Spark Processing Engine loaded {len(data)} rows from raw repository.")
    
    # ETL Transform Aşaması: Sadece Tutarı 3000 TRY'den büyük olan ve Başarılı işlemleri "Riskli" olarak filtrele
    high_risk_transactions = [
        txn for txn in data 
        if txn["amount"] > 3000.0 and txn["status"] == "SUCCESS"
    ]
    
    print(f"Transformation complete. Filtered {len(high_risk_transactions)} high-risk transactions.")
    
    # ETL Load Aşaması: PostgreSQL Metadata DB'ye yazma
    conn = connect(POSTGRES_CONN_STR)
    cursor = conn.cursor()
    
    # Tabloyu otomatik oluştur
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS high_risk_transactions (
            transaction_id VARCHAR(50) PRIMARY KEY,
            customer_id VARCHAR(50),
            amount NUMERIC(10, 2),
            currency VARCHAR(10),
            channel VARCHAR(20),
            status VARCHAR(20),
            processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)
    conn.commit()
    
    # Verileri Insert Et
    for txn in high_risk_transactions:
        cursor.execute("""
            INSERT INTO high_risk_transactions (transaction_id, customer_id, amount, currency, channel, status)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (transaction_id) DO NOTHING;
        """, (txn["transaction_id"], txn["customer_id"], txn["amount"], txn["currency"], txn["channel"], txn["status"]))
        
    conn.commit()
    cursor.close()
    conn.close()
    
    print("Data infrastructure synchronization with PostgreSQL successfully completed.")

# --- ADIM 2: ORKESTRASYON TANIMLAMASI ---

with DAG(
    'enterprise_data_pipeline_orchestration',
    default_args=default_args,
    description='Uctan uca Airflow, MinIO, Spark ve Postgres entegrasyonu',
    schedule_interval=None,
    catchup=False,
    tags=['production', 'etl', 'spark'],
) as dag:

    # 1. Görev: Ham Veriyi Üretip Depolama Alanına At
    ingest_raw_data = PythonOperator(
        task_id='ingest_raw_data_to_lake',
        python_callable=_generate_and_upload_to_minio,
    )

    # 2. Görev: Spark Motoru ile Oku, Dönüştür ve Veritabanına Yükle
    process_and_load_data = PythonOperator(
        task_id='spark_transform_and_load_to_dwh',
        python_callable=_spark_transform_and_load,
    )

    # Bağımlılık Hattı (Pipeline Silsilesi)
    ingest_raw_data >> process_and_load_data