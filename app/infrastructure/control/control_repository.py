from app.infrastructure.db.connection import PostgresConnection
from app.infrastructure.logging.logger import logger

class ControlRepository:
    def __init__(self):
        self.db = PostgresConnection()

    def get_next_pending_chunk(self):

        query = """
            UPDATE control_migracion 
            SET estado = 'EN_PROCESO', 
                fecha_inicio = NOW()
            WHERE nombre_archivo = (
                SELECT nombre_archivo 
                FROM control_migracion 
                WHERE estado = 'PENDIENTE' 
                ORDER BY nombre_archivo ASC 
                LIMIT 1 
                FOR UPDATE SKIP LOCKED
            )
            RETURNING nombre_archivo, total_registros;
        """
        try:
            with self.db.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(query)
                    result = cursor.fetchone()
                    if result:
                        return {
                            "nombre_archivo": result[0], 
                            "total_registros": result[1]
                        }
                    return None
        except Exception as e:
            logger.error("Error obteniendo siguiente chunk", exception=str(e))
            return None

   
    def mark_as_completed(self, nombre_archivo: str, inserted_count: int, monto_total: float):
  
        query = """
            UPDATE control_migracion 
            SET estado = 'OK', 
                registros_insertados = %s,
                monto_total = %s,
                fecha_fin = NOW()
            WHERE nombre_archivo = %s;
        """
        try:
            with self.db.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(query, (inserted_count, monto_total, nombre_archivo))
                    conn.commit()
        except Exception as e:
            logger.error(f"Error al marcar completado {nombre_archivo}", exception=str(e))

    def mark_as_failed(self, nombre_archivo: str, error_msg: str):
        """
        Registra el fallo para que sepas qué arreglar sin detener el worker.
        """
        query = """
            UPDATE control_migracion 
            SET estado = 'ERROR', 
                error_log = %s,
                fecha_fin = NOW()
            WHERE nombre_archivo = %s;
        """
        try:
            with self.db.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(query, (error_msg[:1000], nombre_archivo)) 
                    conn.commit()
        except Exception as e:
            logger.error(f"No se pudo marcar como fallido el archivo {nombre_archivo}", exception=str(e))

    def save_final_audit(self, data: dict):
        """
        Aquí es donde insertas en tu tabla etl_audit_control (la que ya usas).
        """
        query = """
            INSERT INTO etl_audit_control 
            (archivo_nombre, source_rows, source_monto, target_rows, target_monto, diferencia_rows, diferencia_monto, estado)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
        params = (
            data['archivo'], data['s_r'], data['s_m'], 
            data['t_r'], data['t_m'], data['d_r'], data['d_m'], data['est']
        )
        with self.db.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, params)
                conn.commit()