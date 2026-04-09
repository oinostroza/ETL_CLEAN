import pandas as pd
from typing import Dict, Any
from app.infrastructure.logging.logger import logger


def join_datasets(
    transacciones: pd.DataFrame,
    clientes: pd.DataFrame,
    productos: pd.DataFrame
) -> pd.DataFrame:

    logger.info("Realizando joins entre datasets")
    df = transacciones.merge(clientes, on="cliente_id", how="left")
    df = df.merge(productos, on="cliente_id", how="left")
    return df


def aggregate_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Agrega features por cliente: gasto total, número de transacciones.
    """
    logger.info("Agregando features por cliente")
    agg = df.groupby("cliente_id").agg(
        gasto_total=("monto", "sum"),
        num_transacciones=("id_transaccion", "count")
    ).reset_index()
    return df.merge(agg, on="cliente_id", how="left")
