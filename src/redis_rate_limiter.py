import random
import time

import redis


class RateLimitExceed(Exception):
    pass


class RateLimiter:
    """Класс для реализации ограничения запросов по паттерну sliding window"""

    def __init__(self, client: redis.Redis, key="rate_limiter", limit=5, window=3):
        self.client = client
        self.key = key
        self.limit = limit
        self.window = window

    def test(self) -> bool:
        now = time.time()

        pipe = self.client.pipeline()
        pipe.zremrangebyscore(self.key, 0, now - self.window)
        pipe.zcard(self.key)
        removed, count = pipe.execute()

        if count >= self.limit:
            return False

        self.client.zadd(self.key, {str(now): now})

        return True


def make_api_request(rate_limiter: RateLimiter):
    if not rate_limiter.test():
        raise RateLimitExceed
    else:
        # какая-то бизнес логика
        pass


if __name__ == "__main__":
    redis_client = redis.Redis(host="localhost", port=6379, decode_responses=True)
    rate_limiter = RateLimiter(redis_client)

    for _ in range(50):
        time.sleep(random.randint(1, 2))

        try:
            make_api_request(rate_limiter)
        except RateLimitExceed:
            print("Rate limit exceed!")
        else:
            print("All good")
