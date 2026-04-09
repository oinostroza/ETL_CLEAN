import pandas as pd
from app.infrastructure.db.connection import PostgresConnection
from app.infrastructure.logging.logger import logger


def filter_new_records(
    df: pd.DataFrame,
    table: str,
    key_column: str = "id_transaccion"
) -> pd.DataFrame:

    conn = PostgresConnection()
    ids = tuple(df[key_column].unique())
    if not ids:
        return df.iloc[0:0]
    sql = f"SELECT {key_column} FROM {table} WHERE {key_column} IN %s"
    try:
        existing = conn.execute_query(sql, (ids,), fetch_all=True)
        existing_ids = set(row[key_column] for row in existing) if existing else set()
        new_df = df[~df[key_column].isin(existing_ids)]
        logger.info(
            "Filtrado delta",
            total=len(df),
            nuevos=len(new_df),
        )
        return new_df
    except Exception as e:
        logger.error(
            "Error en delta detection",
            exception=e,
        )
        raise
