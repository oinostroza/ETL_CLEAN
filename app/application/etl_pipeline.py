import pandas as pd
from pathlib import Path
from app.config.settings import config
from app.infrastructure.extract.csv_extractor import extract_csv_in_chunks
from app.domain.services.transformations import transform_all
from app.domain.services.data_quality import validate_data
from app.infrastructure.delta.delta_detector import filter_new_records
from app.infrastructure.load.postgres_loader import upsert_to_postgres
from app.infrastructure.load.parquet_loader import save_to_parquet
from app.infrastructure.logging.logger import logger


def run_etl_pipeline():
    logger.info("Iniciando pipeline ETL")
    # Rutas de archivos
    tx_file = str(config.paths.raw_data_dir / "transacciones.csv")
    cl_file = str(config.paths.raw_data_dir / "clientes.csv")
    pr_file = str(config.paths.raw_data_dir / "productos.csv")
    output_parquet = "transacciones_enriquecidas.parquet"
    table_name = "transacciones_enriquecidas"

    clientes = pd.read_csv(cl_file)
    productos = pd.read_csv(pr_file)

    total_inserted = 0
    for chunk in extract_csv_in_chunks(tx_file, chunk_size=config.chunk_size):
        try:
            logger.info("Procesando chunk", rows=len(chunk))

            chunk = validate_data(chunk, key_columns=["id_transaccion", "cliente_id"])
            df = transform_all(chunk, clientes, productos)

            #(delta)
            df_new = filter_new_records(df, table=table_name, key_column="id_transaccion")
            if df_new.empty:
                logger.info("No hay registros nuevos en este chunk")
                continue
   
            inserted = upsert_to_postgres(df_new, table=table_name, unique_key="id_transaccion")
            total_inserted += inserted
       
            save_to_parquet(df_new, output_parquet)
        except Exception as e:
            logger.error("Error procesando chunk", exception=e)
            continue
    logger.info("Pipeline ETL finalizado", total_inserted=total_inserted)
