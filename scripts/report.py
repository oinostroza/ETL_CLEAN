import os
import psycopg2
import pandas as pd

DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "postgres",
    "user": "postgres",
    "password": "1234"
}
PARQUET_DIR = "data/chunks"

def count_db_records():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM transacciones;")
    total = cur.fetchone()[0]
    conn.close()
    return total

def count_parquet_files():
    return len([f for f in os.listdir(PARQUET_DIR) if f.endswith('.parquet')])

def avg_rows_per_chunk():
    files = [f for f in os.listdir(PARQUET_DIR) if f.endswith('.parquet')]
    if not files:
        return 0
    total_rows = 0
    for f in files:
        df = pd.read_parquet(os.path.join(PARQUET_DIR, f), engine='fastparquet')
        total_rows += len(df)
    return total_rows // len(files) if files else 0

def print_sample_data():
    files = [f for f in os.listdir(PARQUET_DIR) if f.endswith('.parquet')]
    if not files:
        print("No hay archivos Parquet para mostrar.")
        return
    df = pd.read_parquet(os.path.join(PARQUET_DIR, files[0]), engine='fastparquet')
    print("\nEjemplo de datos (primer chunk):")
    print(df.head(10).to_string(index=False))

def print_report():
    print("\n==============================")
    print("   RESUMEN DEL PROCESO ETL   ")
    print("==============================")
    print(f"Registros en DB:         {count_db_records():,}")
    print(f"Archivos Parquet:        {count_parquet_files():,}")
    print(f"Promedio filas/chunk:    {avg_rows_per_chunk():,}")
    print_sample_data()
    print("==============================\n")

if __name__ == "__main__":
    print_report()