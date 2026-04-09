import pandas as pd
from typing import List
from app.infrastructure.logging.logger import logger


def validate_data(
    df: pd.DataFrame,
    key_columns: List[str],
    monto_column: str = "monto"
) -> pd.DataFrame:

    logger.info("Validando calidad de datos", rows=len(df))

    df = df.drop_duplicates(subset=key_columns)
    for col in key_columns:
        if df[col].isnull().any():
            logger.warning(f"Nulos detectados en clave: {col}", columna=col)
            df = df[df[col].notnull()]
    
    if monto_column in df.columns:
        df = df[df[monto_column] > 0]
    return df
