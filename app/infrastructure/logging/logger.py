import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional
from app.config.settings import config


class JSONFormatter(logging.Formatter):
    """Formateador que genera logs en formato JSON."""

    def format(self, record: logging.LogRecord) -> str:

        log_data = {
            "timestamp": datetime.utcnow().isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno,
        }

        # Agregar información de excepciones si existe
        if record.exc_info:
            log_data["exception"] = self.formatException(record.exc_info)

        # Agregar campos adicionales si existen
        if hasattr(record, "extra_fields"):
            log_data.update(record.extra_fields)

        return json.dumps(log_data, ensure_ascii=False)


class StructuredLogger:
   
    def __init__(self, name: str = "etl_pipeline"):
   
        self.logger = logging.getLogger(name)
        self.logger.setLevel(logging.DEBUG)

        # Evitar duplicar handlers
        if not self.logger.handlers:
            self._setup_handlers()

    def _setup_handlers(self):
        # Handler para consola
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(JSONFormatter())
        self.logger.addHandler(console_handler)

        # Handler para archivo
        log_file = config.paths.logs_dir / "etl_pipeline.log"
        file_handler = logging.FileHandler(log_file, encoding="utf-8")
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(JSONFormatter())
        self.logger.addHandler(file_handler)

    def log(
        self,
        level: str,
        message: str,
        **kwargs: Any
    ):
        """
        Registra un log con campos adicionales.
        
        Args:
            level: Nivel de log (DEBUG, INFO, WARNING, ERROR, CRITICAL)
            message: Mensaje principal
            **kwargs: Campos adicionales a incluir en el JSON
        """
        record = self.logger.makeRecord(
            self.logger.name,
            getattr(logging, level.upper()),
            "(unknown file)",
            0,
            message,
            (),
            None,
        )
        record.extra_fields = kwargs
        self.logger.handle(record)

    def info(self, message: str, **kwargs):
        """Log de nivel INFO."""
        self.log("INFO", message, **kwargs)

    def error(self, message: str, exception: Optional[Exception] = None, **kwargs):
        """Log de nivel ERROR."""
        if exception:
            kwargs["exception_type"] = type(exception).__name__
            kwargs["exception_message"] = str(exception)
        self.log("ERROR", message, **kwargs)

    def warning(self, message: str, **kwargs):
        """Log de nivel WARNING."""
        self.log("WARNING", message, **kwargs)

    def debug(self, message: str, **kwargs):
        """Log de nivel DEBUG."""
        self.log("DEBUG", message, **kwargs)


logger = StructuredLogger("etl_pipeline")
