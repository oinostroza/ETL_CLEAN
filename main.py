import sys
import os
from app.domain.services.reconciliation_service import ReconciliationService
from app.infrastructure.extract.csv_extractor import CSVExtractor
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

        extractor = CSVExtractor(chunk_size=300000) 
        transformer = PandasTransformer(df_clientes, df_productos)
        delta_detector = DeltaDetector()
        loader = PostgresLoader()
        file_loader = ParquetLoader(base_path="data/processed/transacciones_v1")
        recon_service = ReconciliationService()


        pipeline = ETLPipeline(
            extractor=extractor,
            transformer=transformer,
            delta_detector=delta_detector,
            loader=loader,
            file_loader=file_loader,
            recon_service=recon_service
        )

        # 5. Ejecutar
        pipeline.run(path_raw_tx)
        logger.info("🏁 Proceso ETL finalizado exitosamente.")

    except KeyboardInterrupt:
        logger.warning("⚠️ Proceso interrumpido por el usuario.")
    except Exception as e:
        logger.error(f"💥 Fallo catastrófico en el Main: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()