import pandas as pd
from typing import Set
from app.infrastructure.db.connection import PostgresConnection
from app.infrastructure.logging.logger import logger

class DeltaDetector:
    def __init__(self):
        self.db = PostgresConnection()

    def filter_new_records(
        self, 
        df: pd.DataFrame, 
        table: str, 
        key_column: str = "id_transaccion"
    ) -> pd.DataFrame:

        if df.empty:
            return df

        ids_to_check = tuple(df[key_column].astype(str).unique())
        
        if not ids_to_check:
            return df.iloc[0:0]

        sql = f"SELECT {key_column} FROM {table} WHERE {key_column} IN %s"
        
        try:
            with self.db.get_connection() as conn:      
                with conn.cursor() as cursor:
                    cursor.execute(sql, (ids_to_check,))
                    existing_ids: Set[str] = {str(row[0]) for row in cursor.fetchall()}

            new_df = df[~df[key_column].astype(str).isin(existing_ids)].copy()

            logger.info(
                "Detección de Delta finalizada",
                total_recibido=len(df),
                nuevos_detectados=len(new_df),
                omitidos=len(df) - len(new_df)
            )
            
            return new_df

        except Exception as e:
            logger.error(
                "Error en delta detection",
                table=table,
                exception=str(e)
            )
            raise e