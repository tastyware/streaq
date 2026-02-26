Workers
=======

Worker lifespan
---------------

Workers accept a ``lifespan`` parameter, which allows you to define task dependencies in a type-safe way, as well as run code at startup/shutdown if desired.

First, define the dependencies in a custom ``dataclass`` or ``NamedTuple``:

.. code-block:: python

   from typing import NamedTuple
   from httpx import AsyncClient

   class WorkerContext(NamedTuple):
       """
       Type safe way of defining the dependencies of your tasks.
       e.g. HTTP client, database connection, settings.
       """
       http_client: AsyncClient

Now, tasks will be able to access the ``http_client`` in order to use make requests.

Next, create an async context manager to run at worker creation/teardown. Use this to set up and tear down your dependencies, as well as run extra code if needed.

.. code-block:: python

   from contextlib import asynccontextmanager
   from typing import AsyncGenerator
   from streaq import Worker

   @asynccontextmanager
   async def lifespan() -> AsyncGenerator[WorkerContext, None]:
       # here we run code if desired after worker start up
       # yield our dependencies as an instance of the class
       async with AsyncClient() as http_client:
           yield WorkerContext(http_client)
           # here we run code if desired before worker shutdown

Now, tasks created for the worker will have access to the dependencies like so:

.. code-block:: python

   from streaq import WorkerDepends

   worker = Worker(lifespan=lifespan)

   @worker.task
   async def fetch(url: str, ctx: WorkerContext = WorkerDepends()) -> int:
      res = await ctx.http_client.get(url)
      return len(res.text)

.. important::
   Note that worker dependencies are available in running workers only, **NOT** when using a worker's async context manager, which is for enqueuing tasks.

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
- ``redis_pool``: `coredis connection pool <https://coredis.readthedocs.io/en/latest/handbook/connections.html#connection-pools>`_ for Redis connections
- ``redis_kwargs``: additional arguments for Redis connections
- ``concurrency``: the maximum number of tasks the worker can run concurrently
- ``sync_concurrency``: the maximum number of tasks the worker can run simultaneously in separate threads; defaults to the same as ``concurrency``
- ``queue_name``: name of the queue in Redis, can be used to create multiple queues at once
- ``priorities``: a list of custom priorities for tasks, ordered from lowest to highest
- ``prefetch``: the number of tasks to pre-fetch from Redis, defaults to ``concurrency``. You can set this to ``0`` to disable prefetching entirely.
- ``tz``: ``tzinfo`` controlling the time zone for the worker's cron scheduler and logs
- ``handle_signals``: whether to handle signals for graceful shutdown (unavailable on Windows)
- ``health_crontab``: crontab for frequency to store worker health in Redis
- ``idle_timeout``: the number of seconds to wait before re-enqueuing idle tasks (either prefetched tasks that don't run, or running tasks that become unresponsive)
- ``grace_period``: the number of seconds after receiving SIGINT or SIGTERM to wait for tasks to finish before performing a hard shutdown
- ``anyio_backend``: either trio or asyncio, defaults to asyncio
- ``anyio_kwargs``: extra arguments for anyio, see documentation `here <https://anyio.readthedocs.io/en/stable/basics.html#backend-specific-options>`_
- ``sentinel_kwargs``: extra arguments to pass to sentinel connections (see below)
- ``id``: a custom worker ID. You must ensure that it is unique for the specified queue name.

Deploying with Redis Sentinel
-----------------------------

In production environments, high availability guarantees are often needed, which is why Redis Sentinel was created. streaQ allows you to use Redis Sentinel easily:

.. code-block:: python

   worker = Worker(
       sentinel_master="mymaster",
       sentinel_nodes=[
           ("localhost", 26379),
           ("localhost", 26380),
           ("localhost", 26381),
       ],
   )

For a simple Docker Compose file to get a cluster running, see `here <https://gist.github.com/Graeme22/d2745077ad62a08e3fcf5f71c6b5b431>`_.

Deploying with Redis Cluster
----------------------------

Redis Cluster support is also provided, with some caveats. streaQ makes heavy use of Redis pipelines and Lua scripting, which are difficult to support on cluster. However, it is still possible to use a cluster environment by using a `hash tag <https://redis.io/docs/latest/operate/rc/databases/configuration/clustering/#manage-the-hashing-policy>`_, which guarantees that different keys wind up on the same node. A hash tag is defined with curly braces:

.. code-block:: python

   worker = Worker(cluster_nodes=[("localhost", 7000)], queue_name="{default}")

If you use several queues in the same cluster (which is the only way to scale), you should use different prefixes so that the queues are evenly distributed across cluster nodes to improve performance.

For a simple Docker Compose file to get a cluster running, see `here <https://gist.github.com/Graeme22/d2745077ad62a08e3fcf5f71c6b5b431>`_.

Modularizing task definitions
-----------------------------

Sometimes in large apps, registering all tasks to a single, global ``Worker`` instance is not feasible (or at the very least, cumbersome). streaQ solves this problem by allowing you to create multiple separate ``Worker`` instances and eventually combine them together:

.. code-block:: python
   :caption: other.py

   from streaq import Worker

   other = Worker()

   @other.task
   async def foobar() -> bool: ...

.. code-block:: python
   :caption: main.py

   from anyio import run

   from other import foobar, other
   from streaq import Worker

   worker = Worker()
   worker.include(other)

   @worker.task
   async def barfoo() -> bool: ...

   async def main():
       async with worker:
           await foobar.enqueue()
           await barfoo.enqueue()

   if __name__ == "__main__":
       run(main)

This allows for grouping tasks in whatever way you choose. We now have a task, ``foobar``, which is defined independently of our main worker and can be enqueued without it as well. Importantly, tasks defined without awareness of the main worker can still access its dependencies thanks to dependency injection:

.. code-block:: python

   from sqlalchemy.ext.asyncio import AsyncSession

   class WorkerContext(NamedTuple):
       db: AsyncSession

   @other.task
   async def access_database(ctx: WorkerContext = WorkerDepends()) -> None:
       ...
       await ctx.db.commit()

Separating enqueuing from task definitions
------------------------------------------

A common scenario is to have separate codebases for the backend and the worker. For example, if your worker is serving a large LLM, you probably don't want to load the LLM in the backend. There are two ways to handle this:

First, you can simply use type stubs to re-define the task signatures in the backend:

.. code-block:: python

   from streaq import Worker

   # this worker should have the same Redis URL, serializer/deserializer, signing key,
   # and queue name as the worker defined elsewhere
   worker = Worker(redis_url="redis://localhost:6379")

   @worker.task
   async def fetch(url: str) -> int: ...

Now, tasks can be enqueued in the same way as before:

.. code-block:: python

   async with worker:
       await fetch.enqueue("https://github.com/tastyware/streaq")

The second way is to use ``Worker.enqueue_unsafe``:

.. code-block:: python

   from streaq import Worker

   # again, this worker should have the same Redis URL, serializer/deserializer,
   # signing key, and queue name as the worker defined elsewhere
   worker = Worker(redis_url="redis://localhost:6379")

   async with worker:
       await worker.enqueue_unsafe("fetch", "https://tastyware.dev")

This method is not type-safe, but it doesn't require you to re-define the task signature in the backend. Here, the first parameter is the ``fn_name`` of the task defined elsewhere, and the rest of the args and kwargs can be passed normally.

.. important::

    If using the (default) pickle serializer/deserializer, the result/exception objects used must be accessible to both the backend and worker as well.

    This may be done by moving them to a shared module, copying them between codebases, or just using builtin ones.

Task-related functions
----------------------

Sometimes you'll want to abort tasks, fetch task info, etc. without having access to the original task object. This can be done easily:

.. code-block:: python

   async with worker:
       print(await worker.status_by_id(my_task_id))
       print(await worker.result_by_id(my_task_id))
       print(await worker.info_by_id(my_task_id))
       print(await worker.abort_by_id(my_task_id))
       await worker.unschedule_by_id(my_task_id)

You can also fetch all tasks of a given status:

.. code-block:: python

   from streaq.task import TaskStatus

   await worker.get_tasks_by_status(TaskStatus.RUNNING, limit=10)

Worker optimization
-------------------

If worker performance is important for you, there are a couple optimizations you can make:

* coredis has an `optimized mode <https://coredis.readthedocs.io/en/latest/handbook/optimization.html>`_ that improves performance, mostly by disabling some runtime type checking.

* streaQ supports ``uvloop`` (or ``winloop`` on Windows) for the asyncio backend. In both cases simply install the package and set ``anyio_kwargs={"use_uvloop": True}`` in your worker.
