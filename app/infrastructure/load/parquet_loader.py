import os
import pandas as pd
from app.infrastructure.logging.logger import logger

class ParquetLoader:
    def __init__(self, base_path: str = "data/processed/transacciones"):
        self.base_path = base_path
        os.makedirs(self.base_path, exist_ok=True)

    def load(self, df: pd.DataFrame, chunk_index: int):
        if df.empty:
            return

        file_name = f"chunk_{chunk_index:04d}.parquet"
        file_path = os.path.join(self.base_path, file_name)

        try:
            # Guardamos con compresión snappy (balance ideal entre velocidad y peso)
            # df.to_parquet(file_path, engine='pyarrow', compression='snappy', index=False)
            logger.info(f"💾 Chunk {chunk_index} respaldado en Parquet: {file_path}")
        except Exception as e:
            logger.error(f"Error guardando Parquet en bloque {chunk_index}: {e}")
