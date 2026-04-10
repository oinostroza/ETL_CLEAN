import pandas as pd
from typing import Iterator
from app.config import settings
from app.infrastructure.logging.logger import logger

class CSVExtractor:

  
    def __init__(self, chunk_size: int = None):
        self.chunk_size = chunk_size or getattr(settings, "CHUNK_SIZE", 100000)

    def extract(self, file_path: str, **kwargs) -> Iterator[pd.DataFrame]:
  
        logger.info(
            "Iniciando extracción por chunks",
            file_path=file_path,
            chunk_size=self.chunk_size
        )

        try:
       
            reader = pd.read_csv(
                file_path, 
                chunksize=self.chunk_size,
                low_memory=False, 
                engine='c',
                **kwargs
            )

            for i, chunk in enumerate(reader):
                logger.info(
                    "Chunk cargado a memoria",
                    index=i,
                    rows=len(chunk)
                )
                yield chunk

        except FileNotFoundError:
            logger.error("Archivo no encontrado", file_path=file_path)
            raise
        except Exception as e:
            logger.error("Error durante la iteración del CSV", exception=str(e))
            raise