import io
import pandas as pd
from app.infrastructure.db.connection import PostgresConnection
from app.infrastructure.logging.logger import logger
from app.infrastructure.retry.retry_handler import retry

class PostgresLoader:
    def __init__(self):
        self.db = PostgresConnection()

    def save_audit(self, data: dict):
        sql = """
                INSERT INTO etl_audit_control 
                (archivo_nombre, source_rows, source_monto, target_rows, target_monto, diferencia_rows, diferencia_monto, estado)
                VALUES (%(archivo)s, %(s_r)s, %(s_m)s, %(t_r)s, %(t_m)s, %(d_r)s, %(d_m)s, %(est)s)
            """ 
        try:
            with self.db.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(sql, data)
                    conn.commit()
        except Exception as e:
            logger.error(f"❌ Error al guardar auditoría: {str(e)}")
          
    def get_db_totals(self, table: str):
        sql = f"SELECT COUNT(*), SUM(monto) FROM {table}"
        with self.db.get_connection() as conn:
            with conn.cursor() as cursor:
                    cursor.execute(sql)
                    result = cursor.fetchone()

        return result[0], float(result[1] or 0)
    
    def load(self, df: pd.DataFrame, table: str) -> int:
        if df.empty:
            return 0

        try:
            with self.db.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(f"""
                        SELECT column_name FROM information_schema.columns 
                        WHERE table_name = '{table}'
                    """)
                    db_cols = [row[0] for row in cursor.fetchall()]

                    cols_to_use = [c for c in df.columns if c in db_cols]         
                    if not cols_to_use:
                        logger.warning(f"⚠️ Ninguna columna coincide para la tabla {table}")
                        return 0

                    df_final = df[cols_to_use].copy()

                    output = io.StringIO()
                    df_final.to_csv(
                        output, 
                        sep='|', 
                        header=False, 
                        index=False, 
                        na_rep='NULL'
                    )
                    output.seek(0)


                    temp_table = f"temp_{table}_{int(pd.Timestamp.now().timestamp())}"
                    cursor.execute(f"CREATE TEMP TABLE {temp_table} (LIKE {table} INCLUDING ALL)")            
                    cursor.copy_from(output, 
                                     temp_table, 
                                     sep='|', 
                                     columns=cols_to_use, 
                                     null='NULL')

                    insert_sql = f"""
                        INSERT INTO {table} ({', '.join(cols_to_use)})
                        SELECT {', '.join(cols_to_use)} FROM {temp_table}
                        ON CONFLICT (id_transaccion) DO NOTHING;
                    """
                    cursor.execute(insert_sql)
                    rows_inserted = cursor.rowcount
                    conn.commit()

            return rows_inserted

        except Exception as e:
            logger.error(f"❌ Error en carga: {str(e)}")
            raise e
    #@retry(retries=3, delay=2)
    def load_old(self, df: pd.DataFrame, table: str) -> int:
        if df.empty:
            logger.info(f"DataFrame vacío para tabla {table}, saltando carga.")
            return 0

        try:
            with self.db.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(f"""
                        SELECT column_name 
                        FROM information_schema.columns 
                        WHERE table_name = '{table}'
                    """)
                    db_cols = [row[0] for row in cursor.fetchall()]

                    if not db_cols:
                        raise Exception(f"La tabla '{table}' no existe o no tiene columnas.")

                    cols_to_use = [c for c in df.columns if c in db_cols]
                    df_final = df[cols_to_use]

                    if df_final.empty:
                        logger.warning(f"Ninguna columna del DataFrame coincide con la tabla {table}")
                        return 0

                    output = io.StringIO()
                    df_final.to_csv(
                        output, 
                        sep='|', 
                        header=False, 
                        index=False, 
                        na_rep=''
                    )
                    output.seek(0)

                    cursor.copy_from(
                        file=output,
                        table=table,
                        sep='|',
                        columns=cols_to_use,
                        null=''
                    )
                    
                    conn.commit()

            rows_loaded = len(df_final)
            logger.info(
                f"✅ Carga masiva exitosa en '{table}': {rows_loaded:,} registros insertados."
            )
            return rows_loaded

        except Exception as e:
            logger.error(
                f"❌ Error crítico en carga masiva (COPY) hacia '{table}': {str(e)}"
            )
            raise e