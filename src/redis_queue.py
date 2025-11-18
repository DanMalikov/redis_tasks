import json

import redis


class RedisQueue:
    """Класс очереди"""

    def __init__(self, client: redis.Redis, queue_name: str = "redis_queue"):
        self.client = client
        self.queue_name = queue_name

    def publish(self, msg: dict):
        self.client.rpush(self.queue_name, json.dumps(msg))

    def consume(self) -> dict:
        _, res = self.client.blpop([self.queue_name], 10)
        return json.loads(res)


if __name__ == "__main__":
    redis_client = redis.Redis(host="localhost", port=6379, decode_responses=True)

    q = RedisQueue(client=redis_client)
    q.client.delete(q.queue_name)

    q.publish({"a": 1})
    q.publish({"b": 2})
    q.publish({"c": 3})

    assert q.consume() == {"a": 1}
    assert q.consume() == {"b": 2}
    assert q.consume() == {"c": 3}
