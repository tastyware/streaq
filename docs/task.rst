Tasks
=====

Registering tasks
-----------------

In order to run tasks, they must first be registered with the worker. Let's assume we have a worker that looks like this:

.. code-block:: python

   from streaq import Worker
   worker = Worker(redis_url="redis://localhost:6379")

We can now register async functions with the worker:

.. code-block:: python

   @worker.task()
   async def sleeper(ctx: WrappedContext[None], time: int) -> int:
       await asyncio.sleep(time)
       return time

.. note::
   The first parameter, ``ctx``, is required, even if you don't use it. It will be prepended to the rest of the parameters upon execution. The type of ``ctx`` is covered in the :doc:`worker docs <worker>`.

The ``task`` decorator has several optional arguments that can be used to customize behavior:

- ``max_tries``: maximum number of attempts before giving up if task is retried; defaults to 3
- ``timeout``: amount of time to run the task before raising ``asyncio.TimeoutError``; ``None`` (the default) means never timeout
- ``ttl``: amount of time to store task result in Redis; defaults to 5 minutes. ``None`` means never delete results, ``0`` means never store results
- ``unique``: whether to allow more than one instance of the task to run simultaneously; defaults to ``False`` for normal tasks and ``True`` for cron jobs

Enqueuing tasks
---------------

Once registered, tasks can then be queued up for execution by worker processes:

.. code-block:: python

   async with worker:  # required to enqueue tasks
       # these two are equivalent
       await sleeper.enqueue(5)
       await sleeper.enqueue(5).start()

.. note::
   Everything is type-safe here--you'll get an error if you pass the wrong number or type of parameters! The ``ctx`` parameter is handled by the worker, so you don't pass it in.

We can also defer task execution to a later time:

.. code-block:: python

   from datetime import datetime

   async with worker:
       await sleeper.enqueue(3).start(delay=10)  # start after 10 seconds
       await sleeper.enqueue(3).start(schedule=datetime(...))  # start at a specific time

Task status & results
---------------------

Enqueued tasks return a ``Task`` object which can be used to wait for task results or view the task's status:

.. code-block:: python

   from datetime import timedelta

   async with worker:
       task = await sleeper.enqueue(3).start(delay=timedelta(seconds=5))
       print(await task.status())
       print(await task.result())
       print(await task.status())

.. code-block:: python

   TaskStatus.SCHEDULED
   TaskResult(success=True, result=3, start_time=1740763805099, finish_time=1740763808102, queue_name='streaq')
   TaskStatus.DONE

The ``TaskResult`` object contains information about the task, such as start/end time. The ``success`` flag will tell you whether the object stored in ``result`` is the result of task execution (if ``True``) or an exception raised during execution (if ``False``).

Retrying tasks
--------------

streaQ provides a special exception that you can raise manually inside of your tasks to make sure that they're retried (as long as ``tries <= max_tries`` for that task):

.. code-block:: python

   from streaq.task import StreaqRetry

   @worker.task()
   async def retry(ctx: WrappedContext[None]) -> bool:
       if ctx.tries < 3:
           raise StreaqRetry("Retrying!")
       return True

By default, the retries will use an exponential backoff, where each retry happens after a ``try**2`` second delay. To change this behavior, you can pass the ``delay`` parameter to the ``StreaqRetry`` exception.

.. note::
   streaQ's default behavior when tasks fail is to save the exception raised as the task's result. The exception to this is when a worker is shutdown unexpectedly; when that happens, running tasks will be re-enqueued.

Cancelling tasks
----------------

Tasks that are running or enqueued can be aborted manually:

.. code-block:: python

   async with worker:
       task = await sleeper.enqueue(3)
       await task.abort()

Here, the result of the ``abort`` call will be a boolean representing whether the task was successfully cancelled.

Cron jobs
---------

streaQ also includes cron jobs, which allow you to run code at regular, scheduled intervals. You can register a cron job like this:

.. code-block:: python

   # 9:30 on weekdays
   @worker.cron("30 9 * * mon-fri")
   async def cron(ctx: WrappedContext[None]) -> None:
       print("Itsa me, Mario!")

The ``cron`` decorator has one required parameter, the crontab to use which follows the format specified `here <https://github.com/josiahcarlson/parse-crontab?tab=readme-ov-file#description>`_, as well as many of the same optional parameters as the ``task`` decorator.

The timezone used for the scheduler can be controlled via the worker's ``tz`` parameter.

Synchronous calls
-----------------

Functions that can block the loop for extended periods should be run in an executor like ``concurrent.futures.ThreadPoolExecutor`` or ``concurrent.futures.ProcessPoolExecutor``:

.. code-block:: python

   import asyncio
   import time
   from concurrent.futures import ProcessPoolExecutor
   from contextlib import asynccontextmanager
   from dataclasses import dataclass
   from functools import partial
   from typing import AsyncIterator
   from streaq import Worker, WrappedContext

   @dataclass
   class Context:
       pool: ProcessPoolExecutor

   @asynccontextmanager
   async def lifespan(worker: Worker) -> AsyncIterator[Context]:
       with ProcessPoolExecutor() as executor:
           yield Context(executor)

   worker = Worker(worker_lifespan=lifespan)

   def sync_sleep(seconds: int) -> int:
       time.sleep(seconds)
       return seconds

   @worker.task()
   async def do_work(ctx: WrappedContext[Context], seconds: int) -> int:
       loop = asyncio.get_running_loop()
       blocking = partial(sync_sleep, seconds)
       return await loop.run_in_executor(ctx.deps.pool, blocking)
