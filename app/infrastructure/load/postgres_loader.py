import io
import pandas as pd
from app.infrastructure.db.connection import PostgresConnection
from app.infrastructure.logging.logger import logger
from app.infrastructure.retry.retry_handler import retry

class PostgresLoader:
    def __init__(self):
        self.db = PostgresConnection()

    @retry(retries=3, delay=2)
    def load(self, df: pd.DataFrame, table: str) -> int:
  
        if df.empty:
            logger.info("DataFrame vacío, saltando carga", table=table)
            return 0

        buffer = io.StringIO()
        df.to_csv(buffer, index=False, header=False, sep='|', na_rep='')
        buffer.seek(0)

        try:
      
            conn = self.db.get_connection()
            with conn.cursor() as cursor:
  
                cursor.copy_from(
                    file=buffer,
                    table=table,
                    sep='|',
                    columns=list(df.columns),
                    null=''
                )
                conn.commit()
                
            rows_loaded = len(df)
            logger.info(
                "Carga masiva exitosa vía COPY",
                table=table,
                rows=rows_loaded
            )
            return rows_loaded

        except Exception as e:
            logger.error(
                "Error crítico en carga masiva (COPY)",
                table=table,
                exception=str(e)
            )
         
            raise e