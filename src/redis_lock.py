import datetime
import logging
import time
import uuid
from functools import wraps
from typing import Any, Callable

import redis

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

redis_client = redis.Redis(host="localhost", port=6379)

REMOVE_LOCK_SCRIPT = redis_client.register_script("""
if redis.call("get", KEYS[1]) == ARGV[1] then
    return redis.call("del", KEYS[1])
else
    return 0
end
""")


def single(max_processing_time: datetime.timedelta) -> Callable:
    """
    Декоратор с распределенным локом
    Гарантирует, что декорируемая функция не исполняется параллельно
    """

    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            lock_key = f"single_lock:{func.__name__}"
            token = str(uuid.uuid4())
            print(f"{lock_key}")
            ttl_ms = int(max_processing_time.total_seconds() * 1000)

            acquired = redis_client.set(lock_key, token, nx=True, px=ttl_ms)

            if not acquired:
                raise RuntimeError("Функция уже запущена")

            try:
                return func(*args, **kwargs)
            finally:
                REMOVE_LOCK_SCRIPT(keys=[lock_key], args=[token])

        return wrapper

    return decorator


@single(max_processing_time=datetime.timedelta(minutes=2))
def process_transaction():
    logger.info("Начало работы")
    time.sleep(2)
    logger.info("Работа завершена")


if __name__ == "__main__":
    try:
        process_transaction()
    except RuntimeError as e:
        print(e)
