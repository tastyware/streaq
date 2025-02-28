Workers
=======

Worker lifespan
---------------

Workers accept a ``worker_lifespan`` parameter, which allows you to define task dependencies in a type-safe way, as well as run code at startup/shutdown if desired.

First, define the dependencies in a custom class:

.. code-block:: python

   from dataclasses import dataclass
   from httpx import AsyncClient

   @dataclass
   class Context:
       """
       Type safe way of defining the dependencies of your tasks.
       e.g. HTTP client, database connection, settings.
       """
       http_client: AsyncClient

Now, tasks will be able to access the ``http_client`` in order to use API endpoints.

Next, create an async context manager to run at worker creation/teardown. Use this to set up and tear down your dependencies, as well as run extra code if needed.

.. code-block:: python

   from contextlib import asynccontextmanager
   from typing import AsyncIterator
   from streaq import Worker

   @asynccontextmanager
   async def worker_lifespan(worker: Worker) -> AsyncIterator[Context]:
       # here we run code if desired before the worker start up
       await worker.redis.sadd("workers", worker.id)
       # here we yield our dependencies as an instance of the class
       # we created above
       async with AsyncClient() as http_client:
           yield Context(http_client)
       # here we run code if desired after worker shutdown
       await worker.redis.srem("workers", worker.id)

Now, tasks created for the worker will have access to the dependencies like so:

.. code-block:: python

   from streaq import WrappedContext

   worker = Worker(worker_lifespan=worker_lifespan)
   @worker.task()
   async def fetch(ctx: WrappedContext[Context], url: str) -> int:
      res = await ctx.deps.http_client.get(url)
      return len(res.text)

Here, ``ctx``, of type ``WrappedContext``, will have the ``deps`` property which will be of type ``Context``--and all of this is enforced by static typing!

Custom serializer/deserializer
------------------------------

If desired, you can use a custom serializing scheme for speed or security reasons:

.. code-block:: python

   import json

   worker = Worker(serializer=json.dumps, deserializer=json.loads)

Task lifespan
-------------

You can define an async context manager to wrap task execution:

.. code-block:: python

   from contextlib import asynccontextmanager
   from streaq import WrappedContext

   @asynccontextmanager
   async def task_lifespan(ctx: WrappedContext[Context]) -> AsyncIterator[None]:
       print(f"attempt number {ctx.tries} for task {ctx.task_id}")
       yield
       print(f"finished task {ctx.task_id} in worker {ctx.worker_id}")


Other configuration options
---------------------------

``Worker`` accepts a variety of other configuration options:

- ``redis_url``: the URI for connecting to your Redis instance
- ``concurrency``: the maximum number of tasks the worker can run concurrently; by default, this also controls the number of tasks which will be pre-fetched by the worker
- ``queue_fetch_limit``: the number of tasks to pre-fetch from Redis, defaults to ``concurrency * 2``
- ``tz``: ``tzinfo`` controlling the time zone for the worker's cron scheduler
- ``queue_name``: name of the queue in Redis, can be used to create multiple queues at once
- ``health_check_interval``: how often to log info about worker and Redis health (also stored in Redis)
