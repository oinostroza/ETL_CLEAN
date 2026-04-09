from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import sum as _sum, count as _count
from app.config.settings import config
from app.infrastructure.logging.logger import logger


def get_spark_session(app_name: str = "ETLBank") -> SparkSession:
    return SparkSession.builder.master(config.spark_master).appName(app_name).getOrCreate()

def join_datasets_spark(
    transacciones: DataFrame,
    clientes: DataFrame,
    productos: DataFrame
) -> DataFrame:

    logger.info("Realizando joins Spark entre datasets")
    df = transacciones.join(clientes, "cliente_id", "left")
    df = df.join(productos, "cliente_id", "left")
    return df


def aggregate_features_spark(df: DataFrame) -> DataFrame:

    logger.info("Agregando features Spark por cliente")
    agg = df.groupBy("cliente_id").agg(
        _sum("monto").alias("gasto_total"),
        _count("id_transaccion").alias("num_transacciones")
    )
    return df.join(agg, "cliente_id", "left")
