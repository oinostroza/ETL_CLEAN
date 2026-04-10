import pandas as pd
import numpy as np
import re
from app.infrastructure.logging.logger import logger

class PandasTransformer:
    def __init__(self, clientes_df: pd.DataFrame, productos_df: pd.DataFrame):
        self.clientes = clientes_df
        self.productos = productos_df

    def _clean_monto(self, value):
        if pd.isna(value) or value == "":
            return np.nan
        if isinstance(value, str):
            value = value.replace('.', '').replace(',', '.')
            value = re.sub(r'[^0-9.]', '', value)
        try:
            return float(value)
        except ValueError:
            return np.nan

    def transform(self, chunk_df: pd.DataFrame) -> pd.DataFrame:

        df = chunk_df.copy()
        df['monto'] = df['monto'].apply(self._clean_monto)
        df['monto'] = df.groupby('cliente_id')['monto'].transform(
            lambda x: x.fillna(x.mean() if not x.isnull().all() else 0)
        )
        
        df['id_transaccion'] = pd.to_numeric(df['id_transaccion'], errors='coerce')

        if 'canal' in df.columns:
            df['canal'] = df['canal'].str.strip().str.upper()

        df = self._join_datasets(df)
        df = self._aggregate_features(df)
        df = df.drop_duplicates(subset=['id_transaccion'])
        df['ingreso_mensual'] = pd.to_numeric(df['ingreso_mensual'], errors='coerce')

        return df

    def _join_datasets(self, df: pd.DataFrame) -> pd.DataFrame:
        logger.info("Realizando joins enriquecidos")
        df['cliente_id'] = pd.to_numeric(df['cliente_id'], errors='coerce')
        
        df = df.merge(self.clientes, on="cliente_id", how="left")
        df = df.merge(self.productos, on="cliente_id", how="left")
        return df

    def _aggregate_features(self, df: pd.DataFrame) -> pd.DataFrame:
        logger.info("Calculando métricas de comportamiento")
        agg = df.groupby("cliente_id").agg(
            gasto_total=("monto", "sum"),
            num_transacciones=("id_transaccion", "count")
        ).reset_index()
        
        return df.merge(agg, on="cliente_id", how="left", suffixes=('', '_stats'))