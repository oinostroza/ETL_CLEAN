import pandas as pd

class ReconciliationService:
    def __init__(self):
        self.src_rows = 0
        self.src_monto = 0.0

    def compute_source_chunk(self, df: pd.DataFrame):
        self.src_rows += len(df)

        if 'monto' in df.columns:
            monto_ser = df['monto'].astype(str).str.replace('.', '', regex=False).str.replace(',', '.', regex=False)
            monto_numeric = pd.to_numeric(monto_ser, errors='coerce').fillna(0)
            self.src_monto += monto_numeric.sum()     

    def get_source_total(self):
        return self.src_rows, round(self.src_monto, 2)