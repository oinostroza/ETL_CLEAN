import pandas as pd
from app.config.settings import config
from app.infrastructure.logging.logger import logger


def save_to_parquet(df: pd.DataFrame, file_name: str):
    
    output_path = config.paths.output_dir / file_name
    try:
        df.to_parquet(output_path, index=False)
        logger.info(
            "Archivo Parquet guardado",
            output_path=str(output_path),
            rows=len(df),
        )
    except Exception as e:
        logger.error(
            "Error guardando Parquet",
            output_path=str(output_path),
            exception=e,
        )
