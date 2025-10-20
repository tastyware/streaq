Workers
=======

Worker lifespan
---------------

Workers accept a ``lifespan`` parameter, which allows you to define task dependencies in a type-safe way, as well as run code at startup/shutdown if desired.

First, define the dependencies in a custom class:

.. code-block:: python

   from dataclasses import dataclass
   from httpx import AsyncClient

   @dataclass
   class WorkerContext:
       """
       Type safe way of defining the dependencies of your tasks.
       e.g. HTTP client, database connection, settings.
       """
       http_client: AsyncClient

Now, tasks will be able to access the ``http_client`` in order to use API endpoints.

Next, create an async context manager to run at worker creation/teardown. Use this to set up and tear down your dependencies, as well as run extra code if needed.

.. code-block:: python

   from contextlib import asynccontextmanager
   from typing import AsyncGenerator
   from streaq import Worker

   @asynccontextmanager
   async def lifespan() -> AsyncGenerator[WorkerContext]:
       # here we run code if desired after worker start up
       # yield our dependencies as an instance of the class
       async with AsyncClient() as http_client:
           yield WorkerContext(http_client)
           # here we run code if desired before worker shutdown

Now, tasks created for the worker will have access to the dependencies like so:

.. code-block:: python

   worker = Worker(lifespan=lifespan)

   @worker.task()
   async def fetch(url: str) -> int:
      res = await worker.context.http_client.get(url)
      return len(res.text)

Custom serializer/deserializer
------------------------------

If desired, you can use a custom serializing scheme for speed or security reasons:

.. code-block:: python

   import json

   worker = Worker(serializer=json.dumps, deserializer=json.loads)

Signature validation before deserialization
-------------------------------------------

Pickle is great for serializing/deserializing Python objects. However, it presents security risks when we're using Redis, as an attacker who gains access to the Redis database would be able to run arbitrary code. You can protect against this attack vector by passing a ``signing_secret`` to the worker. The signing key ensures corrupted data from Redis will not be unpickled.

.. code-block:: python

   worker = Worker(signing_secret="MY-SECRET-KEY")

The easiest way to generate a new key is with the ``secrets`` module:

.. code-block:: python

   import secrets
   print(secrets.token_urlsafe(32))

Other configuration options
---------------------------

``Worker`` accepts a variety of other configuration options:

- ``redis_url``: the URI for connecting to your Redis instance
- ``redis_kwargs``: additional arguments for Redis connections
- ``concurrency``: the maximum number of tasks the worker can run concurrently
- ``sync_concurrency``: the maximum number of tasks the worker can run simultaneously in separate threads; defaults to the same as ``concurrency``
- ``queue_name``: name of the queue in Redis, can be used to create multiple queues at once
- ``priorities``: a list of custom priorities for tasks, ordered from lowest to highest
- ``prefetch``: the number of tasks to pre-fetch from Redis, defaults to ``concurrency``. You can set this to ``0`` to disable prefetching entirely.
- ``tz``: ``tzinfo`` controlling the time zone for the worker's cron scheduler and logs
- ``handle_signals``: whether to handle signals for graceful shutdown (unavailable on Windows)
- ``health_crontab``: crontab for frequency to store worker health in Redis
- ``idle_timeout``: the amount of time to wait before re-enqueuing idle tasks (either prefetched tasks that don't run, or running tasks that become unresponsive)
- ``anyio_backend``: either trio or asyncio, defaults to asyncio
- ``anyio_kwargs``: extra arguments for anyio, see documentation `here <https://anyio.readthedocs.io/en/stable/basics.html#backend-specific-options>`_
- ``sentinel_kwargs``: extra arguments to pass to sentinel connections (see below)
- ``id``: a custom worker ID. You must ensure that it is unique for the specified queue name.

Deploying with Redis Sentinel
-----------------------------

In production environments, high availability guarantees are often needed, which is why Redis Sentinel was created. streaQ allows you to use Redis Sentinel easily:

.. code-block:: python

   worker = Worker(
       redis_sentinel_master="mymaster",
       redis_sentinel_nodes=[
           ("sentinel-1", 26379),
           ("sentinel-2", 26379),
           ("sentinel-3", 26379),
       ],
   )

If you pass in the ``redis_sentinel_nodes`` parameter, you no longer need to pass ``redis_url``. For a simple Docker Compose script to get a cluster running, see `here <https://gist.github.com/Graeme22/f54800a410757242dbce8e745fca6316>`_.

Redis Cluster is not supported, since streaQ makes heavy use of Redis pipelines and Lua scripting, which are difficult to support on Redis Cluster. For scaling beyond a single Redis instance, it's recommended to use a separate queue for each instance and assign workers to each queue.
