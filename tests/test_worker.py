from streaq.worker import Worker


async def test_redis(worker: Worker):
    await worker.redis.ping()
