import argparse
from app.application.etl_pipeline import run_etl_pipeline
from app.infrastructure.logging.logger import logger

def main():
    parser = argparse.ArgumentParser(description="Plataforma ETL Bancaria Escalable")
    parser.add_argument(
        "--run",
        action="store_true",
        help="Ejecuta el pipeline ETL completo"
    )
    args = parser.parse_args()

    if args.run:
        logger.info("Iniciando pipeline ETL desde CLI")
        run_etl_pipeline()
        logger.info("Pipeline ETL finalizado desde CLI")
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
