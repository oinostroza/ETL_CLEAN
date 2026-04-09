import pandas as pd
from typing import List
from app.infrastructure.db.connection import PostgresConnection
from app.infrastructure.logging.logger import logger
from app.infrastructure.retry.retry_handler import retry


def upsert_to_postgres(
    df: pd.DataFrame,
    table: str,
    unique_key: str = "id_transaccion",
    chunk_size: int = 10000
) -> int:

    conn = PostgresConnection()
    inserted = 0
    columns = list(df.columns)
    placeholders = ",".join(["%s"] * len(columns))
    update_stmt = ", ".join([f"{col}=EXCLUDED.{col}" for col in columns if col != unique_key])
    insert_sql = f"""
        INSERT INTO {table} ({', '.join(columns)})
        VALUES ({placeholders})
        ON CONFLICT ({unique_key}) DO UPDATE SET {update_stmt}
    """
    data = [tuple(row) for row in df.values]
    for i in range(0, len(data), chunk_size):
        batch = data[i:i+chunk_size]
        try:
            inserted += conn.execute_batch(insert_sql, batch)
            logger.info(
                "Batch insert exitoso",
                table=table,
                rows=len(batch),
            )
        except Exception as e:
            logger.error(
                "Error en batch insert",
                table=table,
                exception=e,
            )
    return inserted
