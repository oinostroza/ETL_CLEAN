import os
from pathlib import Path
from dataclasses import dataclass
from typing import Optional


@dataclass
class DatabaseConfig:
   
    host: str = os.getenv("DB_HOST", "localhost")
    port: int = int(os.getenv("DB_PORT", 5432))
    database: str = os.getenv("DB_NAME", "postgres")
    user: str = os.getenv("DB_USER", "postgres")
    password: str = os.getenv("DB_PASSWORD", "1234")


@dataclass
class PathConfig:
    """Configuración de rutas de archivos."""
    base_dir: Path = Path(__file__).parent.parent.parent
    raw_data_dir: Path = base_dir / "data" / "raw"
    processed_data_dir: Path = base_dir / "data" / "processed"
    output_dir: Path = base_dir / "data" / "output"
    logs_dir: Path = base_dir / "logs"

    def __post_init__(self):
       
        for path in [self.raw_data_dir, self.processed_data_dir, self.output_dir, self.logs_dir]:
            path.mkdir(parents=True, exist_ok=True)


@dataclass
class ETLConfig:

    # Procesamiento
    chunk_size: int = 50000
    max_retries: int = 3
    retry_delay: float = 2.0
    
    # Backend
    use_spark: bool = os.getenv("USE_SPARK", "false").lower() == "true"
    spark_master: str = os.getenv("SPARK_MASTER", "local[*]")
    
    # Validación
    validate_on_load: bool = True
    allow_null_keys: bool = False
    
    # Objetos de configuración
    db: DatabaseConfig = DatabaseConfig()
    paths: PathConfig = PathConfig()


# Instancia global
config = ETLConfig()
