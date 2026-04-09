import pandas as pd
from app.infrastructure.transform.pandas_transformer import join_datasets, aggregate_features
from app.infrastructure.logging.logger import logger


def transform_all(transacciones: pd.DataFrame, clientes: pd.DataFrame, productos: pd.DataFrame) -> pd.DataFrame:
  
    logger.info("Iniciando transformaciones de negocio")
    df = join_datasets(transacciones, clientes, productos)
    df = aggregate_features(df)
    # Aquí se pueden agregar más features
    return df
