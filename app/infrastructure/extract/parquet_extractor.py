import pandas as pd
from typing import Iterator
from app.config import settings
from app.infrastructure.logging.logger import logger

class ParquetExtractor: # Podrías renombrar la clase o añadirlo a la existente

    def __init__(self, chunk_size: int = None):
        self.chunk_size = chunk_size or getattr(settings, "CHUNK_SIZE", 100000)

    def extract(self, file_path: str) -> pd.DataFrame:
        logger.info(
            "Leyendo archivo Parquet",
            file_path=file_path
        )

        try:
            df = pd.read_parquet(
                file_path, 
                engine='fastparquet'
            )
            
            logger.info(
                "Archivo Parquet cargado",
                rows=len(df)
            )
            return df

        except FileNotFoundError:
            logger.error("Archivo Parquet no encontrado", file_path=file_path)
            raise
        except Exception as e:
            logger.error("Error leyendo Parquet", exception=str(e))
            raise

   