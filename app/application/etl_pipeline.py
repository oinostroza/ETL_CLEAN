from app.infrastructure.logging.logger import logger

class ETLPipeline:
    def __init__(self, extractor, transformer, delta_detector, loader,file_loader):
        self.extractor = extractor
        self.transformer = transformer
        self.delta = delta_detector
        self.loader = loader
        self.file_loader = file_loader 

    def run(self, file_path: str):

        logger.info(f"Iniciando ejecución del Pipeline para: {file_path}")    
        chunks = self.extractor.extract(file_path)

        for i, chunk in enumerate(chunks):
            try:
                logger.info(f"--- Procesando Bloque {i} ---")

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

        logger.info("Pipeline finalizado.")