import pandas as pd
from app.infrastructure.db.connection import PostgresConnection

class ReconciliationService:
    def __init__(self):
        self.db = PostgresConnection()

    def compute_source_chunk(self, df: pd.DataFrame):
        rows = len(df)
        monto = 0.0
        
        if 'monto' in df.columns:
            if df['monto'].dtype == object:
                monto_ser = df['monto'].astype(str).str.replace('.', '', regex=False).str.replace(',', '.', regex=False)
                monto = pd.to_numeric(monto_ser, errors='coerce').fillna(0).sum()
            else:
                monto = df['monto'].sum()
        
        return rows, monto

    def get_source_total(self):

        query = """
            SELECT 
                SUM(total_registros) as rows, 
                SUM(registros_insertados) as inserted 
            FROM control_migracion 
            WHERE estado = 'OK'
        """    
        with self.db.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query)
                res = cursor.fetchone()
                return res[0] or 0, res[1] or 0