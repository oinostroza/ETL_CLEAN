from app.infrastructure.logging.logger import logger

class ETLPipeline:

    def __init__(self, 
                 extractor, 
                 transformer, 
                 loader,
                 recon_service,
                 control_repo): 
        self.extractor = extractor
        self.transformer = transformer
        self.loader = loader
        self.recon_service = recon_service
        self.control = control_repo

    def _print_banner(self, data):
        print("\n" + "="*50)
        print(f"REPORT DE CUADRATURA FINAL - {data['est']}")
        print("="*50)
        print(f"ARCHIVO: {data['archivo']}")
        print(f"FILAS:    Origen {data['s_r']} | Destino {data['t_r']} | Diff: {data['d_r']}")
        print(f"MONTO:    Origen {data['s_m']:,.2f} | Destino {data['t_m']:,.2f}")
        print(f"RESULTADO FINAL: {'✅ EXITOSO' if data['est'] == 'OK' else '❌ REVISAR'}")
        print("="*50 + "\n")

    def perform_final_reconciliation(self, file_label: str):
 
        logger.info("Iniciando cuadratura final de auditoría...")

        try:

            query_control = """
                SELECT 
                    SUM(total_registros) as total_src_rows, 
                    SUM(monto_total) as total_src_monto
                FROM control_migracion 
                WHERE estado = 'OK'
            """
            
            with self.control.db.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(query_control)
                    res_control = cursor.fetchone()
                    src_rows = res_control[0] or 0
                    src_monto = float(res_control[1] or 0.0)


            tgt_rows, tgt_monto = self.loader.get_db_totals(table="transacciones")
            tgt_monto = float(tgt_monto or 0.0)

            diff_rows = src_rows - tgt_rows
            diff_monto = abs(src_monto - tgt_monto)


            if diff_rows == 0 and diff_monto < 0.01:
                estado = "OK"
                logger.info("✅ Cuadratura exitosa: Los totales coinciden perfectamente.")
            else:
                estado = "DESCUADRADO"
                logger.warning(
                    f"❌ Diferencia detectada: Filas diff: {diff_rows}, Monto diff: {diff_monto}"
                )

            audit_data = {
                "archivo": file_label,
                "s_r": src_rows,
                "s_m": round(src_monto, 2),
                "t_r": tgt_rows,
                "t_m": round(tgt_monto, 2),
                "d_r": diff_rows,
                "d_m": round(src_monto - tgt_monto, 2),
                "est": estado
            }

            self.control.save_final_audit(audit_data)
            self._print_banner(audit_data)

        except Exception as e:
            logger.error("Error crítico durante la reconciliación final", exception=str(e))
            raise

    def run(self):
        logger.info("Iniciando Worker: Buscando chunks pendientes...")

        while True:
     
            chunk_info = self.control.get_next_pending_chunk()
            
            if not chunk_info:
                logger.info("🏁 No hay más chunks pendientes por procesar.")
                break
            
            chunk_name = chunk_info['nombre_archivo']
            parquet_path = f"data/chunks/{chunk_name}"
            
            try:
                logger.info(f"🚀 Procesando: {chunk_name}")

                df_chunk = self.extractor.extract(parquet_path)                
                chunk_rows, chunk_monto = self.recon_service.compute_source_chunk(df_chunk)

                transformed_df = self.transformer.transform(df_chunk)
                self.loader.load(transformed_df, table="transacciones")
                    
                self.control.mark_as_completed(
                    nombre_archivo=chunk_name, 
                    inserted_count=len(transformed_df),
                    monto_total=chunk_monto
                )
                logger.info(f"✅ {chunk_name} finalizado. Filas: {chunk_rows}, Monto: ${chunk_monto:,.2f}")


            except Exception as e:
                logger.error(f"❌ Error crítico en {chunk_name}", exception=str(e))
                self.control.mark_as_failed(chunk_name, str(e))
                continue

        # Al terminar todos los pendientes, lanzamos la auditoría final
        self.perform_final_reconciliation("MIGRACION_MASIVA_PARQUET")