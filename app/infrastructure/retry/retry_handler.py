"""
Sistema de reintentos con backoff exponencial.
"""
import time
from typing import Callable, Any, TypeVar
from functools import wraps
from app.infrastructure.logging.logger import logger


T = TypeVar("T")


def retry(
    max_attempts: int = 3,
    delay: float = 2.0,
    backoff_factor: float = 2.0,
    on_exception: tuple = (Exception,),
):
    """
    Decorador para reintentar funciones con backoff exponencial.
    
    Args:
        max_attempts: Número máximo de intentos
        delay: Delay inicial en segundos
        backoff_factor: Factor multiplicativo para el delay
        on_exception: Tupla de excepciones a capturar
        
    Returns:
        Función decorada
    """
    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @wraps(func)
        def wrapper(*args, **kwargs) -> T:
            current_delay = delay
            last_exception = None

            for attempt in range(1, max_attempts + 1):
                try:
                    return func(*args, **kwargs)
                except on_exception as e:
                    last_exception = e
                    
                    if attempt < max_attempts:
                        logger.warning(
                            f"Intento {attempt}/{max_attempts} falló. Reintentando en {current_delay}s",
                            function=func.__name__,
                            attempt=attempt,
                            error=str(e),
                        )
                        time.sleep(current_delay)
                        current_delay *= backoff_factor
                    else:
                        logger.error(
                            f"Todos los {max_attempts} intentos fallaron",
                            function=func.__name__,
                            exception=last_exception,
                        )

            raise last_exception

        return wrapper

    return decorator


class RetryHandler:
    """Manejador centralizado de reintentos."""

    @staticmethod
    def execute_with_retry(
        func: Callable[..., T],
        max_attempts: int = 3,
        delay: float = 2.0,
        *args,
        **kwargs
    ) -> T:
        """
        Ejecuta una función con reintentos.
        
        Args:
            func: Función a ejecutar
            max_attempts: Número máximo de intentos
            delay: Delay inicial
            *args: Argumentos posicionales
            **kwargs: Argumentos nombrados
            
        Returns:
            Resultado de la función
        """
        decorated_func = retry(max_attempts=max_attempts, delay=delay)(func)
        return decorated_func(*args, **kwargs)
