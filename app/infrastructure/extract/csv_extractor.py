import pandas as pd
from typing import Iterator, Dict, Any
from app.config.settings import config
from app.infrastructure.logging.logger import logger


def extract_csv_in_chunks(
    file_path: str,
    chunk_size: int = None,
    **read_csv_kwargs
) -> Iterator[pd.DataFrame]:

    chunk_size = chunk_size or config.chunk_size
    logger.info(
        "Extrayendo CSV por chunks",
        file_path=file_path,
        chunk_size=chunk_size,
    )
    try:
        for chunk in pd.read_csv(file_path, chunksize=chunk_size, **read_csv_kwargs):
            logger.info(
                "Chunk extraído",
                file_path=file_path,
                rows=len(chunk),
            )
            yield chunk
    except Exception as e:
        logger.error(
            "Error extrayendo CSV",
            file_path=file_path,
            exception=e,
        )
        raise
