import sys
import os
import pandas as pd
from app.infrastructure.db.connection import PostgresConnection
from app.infrastructure.logging.logger import logger

db = PostgresConnection()

def registrar_en_control(nombre, total):
    query = """
        INSERT INTO control_migracion (nombre_archivo, total_registros, estado)
        VALUES (%s, %s, 'PENDIENTE')
        ON CONFLICT (nombre_archivo) DO NOTHING; -- Evita errores si reinicias el script
    """
    try:
        with db.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, (nombre, total))
                conn.commit()
    except Exception as e:
        logger.error(f"Error registrando {nombre} en control", exception=str(e))

def main():
    logger.info("🚀 Iniciando Fase de Ingesta: CSV -> Parquet Chunks")
    path_raw_tx = "data/raw/transacciones.csv"
    chunks_dir = 'data/chunks'

    if not os.path.exists(path_raw_tx):
        logger.error(f"Archivo no encontrado: {path_raw_tx}")
        sys.exit(1)

    os.makedirs(chunks_dir, exist_ok=True)
    
    reader = pd.read_csv(
        path_raw_tx, 
        chunksize=300000, 
        low_memory=False
    )
    
    for i, chunk in enumerate(reader):
        file_name = f"chunk_{i:03d}.parquet"
        file_path = os.path.join(chunks_dir, file_name)
        

        if os.path.exists(file_path):
            logger.info(f"⏩ Saltando {file_name}, ya existe en disco.")
            continue

        try:

            chunk['id_transaccion'] = (
                chunk['id_transaccion']
                .str.replace('TX-', '', regex=False)
                .astype(int)
            )
            
            chunk.to_parquet(file_path, index=False, engine='fastparquet')
            registrar_en_control(file_name, len(chunk))
            
            logger.info(f"📦 Generado e Indexado: {file_name} ({len(chunk)} filas)")

        except Exception as e:
            logger.error(f"❌ Fallo al procesar chunk {i}", exception=str(e))
            continue

    logger.info("✅ Fase de Ingesta finalizada. Los workers ya pueden empezar a procesar.")

if __name__ == "__main__":
    main()