from app.infrastructure.logging.logger import logger

class ETLPipeline:
    def __init__(self, 
                 extractor, 
                 transformer, 
                 delta_detector, 
                 loader,
                 file_loader,
                 recon_service):
        self.extractor = extractor
        self.transformer = transformer
        self.delta = delta_detector
        self.loader = loader
        self.file_loader = file_loader
        self.recon_service = recon_service


    def _print_banner(self, data):
        print("\n" + "="*50)
        print(f"REPORT DE CUADRATURA - {data['est']}")
        print("="*50)
        print(f"ARCHIVO: {data['archivo']}")
        print(f"FILAS:    Origen {data['s_r']} | Destino {data['t_r']} | Diff: {data['d_r']}")
        print(f"MONTO:    Origen {data['s_m']:,.2f} | Destino {data['t_m']:,.2f}")
        print(f"RESULTADO FINAL: {'✅ EXITOSO' if data['est'] == 'OK' else '❌ REVISAR'}")
        print("="*50 + "\n")

    def perform_final_reconciliation(self, file_path: str):
        src_rows, src_monto = self.recon_service.get_source_total()
        tgt_rows, tgt_monto = self.loader.get_db_totals(table="transacciones")

        diff_r = src_rows - tgt_rows
        diff_m = abs(src_monto - tgt_monto)
        estado = "OK" if diff_r == 0 and diff_m < 0.01 else "DESCUADRADO"

        audit_data = {
            "archivo": file_path,
            "s_r": src_rows,
            "s_m": src_monto,
            "t_r": tgt_rows,
            "t_m": tgt_monto,
            "d_r": src_rows - tgt_rows,
            "d_m": round(src_monto - tgt_monto, 2),
            "est": estado
        }
        self.loader.save_audit(audit_data)
        self._print_banner(audit_data)
        logger.info("Reconciliación finalizada", audit_data=audit_data)
        
    def run(self, file_path: str):

        logger.info(f"Iniciando ejecución del Pipeline para: {file_path}")    
        chunks = self.extractor.extract(file_path)

        for i, chunk in enumerate(chunks):
            try:
                logger.info(f"--- Procesando Bloque {i} ---")
                self.recon_service.compute_source_chunk(chunk)
                transformed_df = self.transformer.transform(chunk)

                new_records = self.delta.filter_new_records(
                    df=transformed_df,
                    table="transacciones",
                    key_column="id_transaccion"
                )

                if not new_records.empty:
                    self.loader.load(new_records, table="transacciones")
                    self.file_loader.load(new_records, chunk_index=i)
                    logger.info(f"✅ Bloque {i} completado con éxito.")

                else:
                    logger.info(f"⏩ Bloque {i} omitido (todos los registros ya existen).")

            except Exception as e:

                logger.error(
                    f"❌ Error crítico procesando el bloque {i}. Saltando al siguiente...",
                    exception=str(e),
                    block_index=i
                )
                continue
        self.perform_final_reconciliation(file_path)
        logger.info("Pipeline finalizado.")