import psycopg2
from psycopg2.extras import RealDictCursor
from contextlib import contextmanager
from typing import Generator, Optional, List, Dict, Any
from app.config.settings import config
from app.infrastructure.logging.logger import logger
from app.infrastructure.retry.retry_handler import retry


class PostgresConnection:
  
    def __init__(self, db_config=None):
        self.db_config = db_config or config.db

    @retry(max_attempts=3, delay=2.0, on_exception=(psycopg2.OperationalError,))
    def _create_connection(self):
        return psycopg2.connect(
            host=self.db_config.host,
            port=self.db_config.port,
            database=self.db_config.database,
            user=self.db_config.user,
            password=self.db_config.password,
        )

    @contextmanager
    def get_connection(self):

        conn = None
        try:
            conn = self._create_connection()
            yield conn
            conn.commit()
        except Exception as e:
            if conn:
                conn.rollback()
            logger.error("Error en conexión a PostgreSQL", exception=e)
            raise
        finally:
            if conn:
                conn.close()

    @contextmanager
    def get_cursor(self, dict_cursor: bool = False):
        
        with self.get_connection() as conn:
            cursor_class = RealDictCursor if dict_cursor else None
            cursor = conn.cursor(cursor_factory=cursor_class)
            try:
                yield cursor
            finally:
                cursor.close()

    def execute_query(
        self,
        query: str,
        params: tuple = (),
        fetch_all: bool = False,
    ) -> Optional[List[Dict[str, Any]]]:

        try:
            with self.get_cursor(dict_cursor=True) as cursor:
                cursor.execute(query, params)
                if fetch_all:
                    return cursor.fetchall()
                return cursor.fetchone()
        except Exception as e:
            logger.error(
                "Error ejecutando consulta SELECT",
                exception=e,
                query=query,
            )
            raise

    def execute_update(self, query: str, params: tuple = ()) -> int:

        try:
            with self.get_cursor() as cursor:
                cursor.execute(query, params)
                return cursor.rowcount
        except Exception as e:
            logger.error(
                "Error ejecutando consulta INSERT/UPDATE/DELETE",
                exception=e,
                query=query,
            )
            raise

    def execute_batch(self, query: str, data: List[tuple]) -> int:

        try:
            with self.get_cursor() as cursor:
                cursor.executemany(query, data)
                return cursor.rowcount
        except Exception as e:
            logger.error(
                "Error ejecutando batch insert",
                exception=e,
                rows_attempted=len(data),
            )
            raise

    def create_table(self, create_statement: str) -> bool:

        try:
            self.execute_update(create_statement)
            logger.info("Tabla creada exitosamente")
            return True
        except psycopg2.Error as e:
            if "already exists" in str(e):
                logger.info("Tabla ya existe")
                return False
            raise
