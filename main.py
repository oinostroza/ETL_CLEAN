import sys
import os
from app.domain.services.reconciliation_service import ReconciliationService
from app.infrastructure.control.control_repository import ControlRepository
from app.infrastructure.extract.csv_extractor import CSVExtractor
from app.infrastructure.extract.parquet_extractor import ParquetExtractor
from app.infrastructure.load.parquet_loader import ParquetLoader
from app.infrastructure.transform.pandas_transformer import PandasTransformer
from app.infrastructure.delta.delta_detector import DeltaDetector
from app.infrastructure.load.postgres_loader import PostgresLoader
from app.application.etl_pipeline import ETLPipeline
from app.infrastructure.logging.logger import logger
import pandas as pd


def main():
    logger.info("🚀 Iniciando Proceso ETL de Alta Volumetría")

    path_raw_tx = "data/raw/transacciones.csv"
    path_raw_clientes = "data/raw/clientes.csv"
    path_raw_productos = "data/raw/productos.csv"

    for path in [path_raw_tx, path_raw_clientes, path_raw_productos]:
        if not os.path.exists(path):
            logger.error(f"Falta archivo crítico: {path}")
            sys.exit(1)

    try:
        logger.info("Cargando maestros de Clientes y Productos...")
        df_clientes = pd.read_csv(path_raw_clientes)
        df_productos = pd.read_csv(path_raw_productos)

        # extractor = CSVExtractor(chunk_size=300000) 
        extractor = ParquetExtractor()
        transformer = PandasTransformer(df_clientes, df_productos)
        loader = PostgresLoader()
        control_repo = ControlRepository()
        recon_service = ReconciliationService()


        pipeline = ETLPipeline(
            extractor=extractor,
            transformer=transformer,
            loader=loader,
            recon_service=recon_service,
            control_repo=control_repo
        )

        # 5. Ejecutar
        pipeline.run()
        logger.info("🏁 Proceso ETL finalizado exitosamente.")

    except KeyboardInterrupt:
        logger.warning("⚠️ Proceso interrumpido por el usuario.")
    except Exception as e:
        logger.error(f"💥 Fallo catastrófico en el Main: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()