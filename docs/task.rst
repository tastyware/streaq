Tasks
=====

Task execution
--------------

streaQ preserves arq's task execution model, called "pessimistic execution": tasks aren’t removed from the queue until they’ve either succeeded or failed. If the worker shuts down, the task will be cancelled immediately and will remain in the queue to be run again when the worker starts up again (or gets run by another worker which is still running).

In the case of a catastrophic failure (that is, the worker shuts down abruptly without doing cleanup), tasks can usually still be retried as long as you set a ``timeout`` when registering the task.

All streaQ tasks should therefore be designed to cope with being called repeatedly if they’re cancelled. If necessary, use database transactions, idempotency keys or Redis to mark when non-repeatable work has completed to avoid doing it twice.

.. note::
   Idempotency is super easy with Redis, see `here <https://gist.github.com/Graeme22/5cd3bffba46480d3936dad407b14d6a4>`_!

streaQ handles exceptions in the following manner:

* ``StreaqRetry`` exceptions result in retrying the task, sometimes after a delay (see below).
* ``asyncio.CancelledError`` exceptions result in the task failing if the task was aborted by the user, or being retried if the worker was shut down unexpectedly.
* ``asyncio.TimeoutError`` exceptions result in the task failing if the task took too long to run.
* Any other ``Exception`` will result in the task failing.

Registering tasks
-----------------

In order to run tasks, they must first be registered with the worker. Let's assume we have a worker that looks like this:

.. code-block:: python

   from streaq import Worker
   worker = Worker(redis_url="redis://localhost:6379")

We can now register async functions with the worker:

.. code-block:: python

   @worker.task()
   async def sleeper(time: int) -> int:
       await asyncio.sleep(time)
       return time

The ``task`` decorator has several optional arguments that can be used to customize behavior:

- ``max_tries``: maximum number of attempts before giving up if task is retried; defaults to 3
- ``silent``: whether to silence task startup/shutdown logs and task success/failure tracking; defaults to False
- ``timeout``: amount of time to run the task before raising ``asyncio.TimeoutError``; ``None`` (the default) means never timeout
- ``ttl``: amount of time to store task result in Redis; defaults to 5 minutes. ``None`` means never delete results, ``0`` means never store results
- ``unique``: whether to prevent more than one instance of the task running simultaneously; defaults to ``False`` for normal tasks and ``True`` for cron jobs. (Note that more than one instance may be queued, but two running at once will cause the second to fail.)

Enqueuing tasks
---------------

Once registered, tasks can then be queued up for execution by worker processes, with full type safety:

.. code-block:: python

   async with worker:  # required to enqueue tasks
       # these two are equivalent
       await sleeper.enqueue(5)
       await sleeper.enqueue(5).start()

We can also defer task execution to a later time:

.. code-block:: python

   from datetime import datetime

   async with worker:
       await sleeper.enqueue(3).start(delay=10)  # start after 10 seconds
       await sleeper.enqueue(3).start(schedule=datetime(...))  # start at a specific time

Tasks can depend on other tasks, meaning they won't be enqueued until their dependencies have finished successfully. If the dependency fails, the dependent task will not be enqueued.

.. code-block:: python

   async with worker:
       task1 = await sleeper.enqueue(1)
       task2 = await sleeper.enqueue(2).start(after=task1.id)
       task3 = await sleeper.enqueue(3).start(after=[task1.id, task2.id])

Task priorities
---------------

Sometimes, certain critical tasks should "skip the line" and receive priority over other tasks. streaQ supports this by allowing you to specify a priority when enqueuing tasks. If a low priority queue is backed up, you can use a high priority queue to ensure that critical tasks are executed quickly.

By passing the ``priorities`` argument on worker creation, you can create an arbitrary number of queues with your priority ordering. (Please take into account that there will be a slight performance penalty per additional queue.)

.. code-block:: python

   # this list should be ordered from lowest to highest
   worker = Worker(priorities=["low", "high"])

   async with worker:
       await sleeper.enqueue(3).start(priority="low")

Here's an example that demonstrates how priorities work. Note that the low priority task is enqueued first, but the high priority task is executed first. (Make sure to run this *before* starting the worker!)

.. code-block:: python

   worker = Worker(concurrency=1)  # max 1 task running at a time for demo

   @worker.task()
   async def low() -> None:
       print("Low priority task")

   @worker.task()
   async def high() -> None:
       print("High priority task")

   async with worker:
       await low.enqueue().start(priority="low")
       await high.enqueue().start(priority="high")

To organize your priorities efficiently, consider using an enum:

.. code-block:: python

   from enum import Enum

   class TaskPriority(str, Enum):
       LOW = "low"
       MEDIUM = "medium"
       HIGH = "high"

   worker = Worker(priorities=[p.value for p in TaskPriority])

Enqueuing by batch
------------------

For most cases, the above method of enqueuing tasks is sufficient. However, streaQ also provides a way to enqueue a group of tasks together in order to maximize efficiency:

.. code-block:: python

   async with worker:
       # importantly, we're not using `await` here
       tasks = [sleeper.enqueue(i) for i in range(10)]
       await worker.enqueue_many(tasks)

Running tasks locally
---------------------

Sometimes, you may wish to run a task's underlying function directly and skip enqueuing entirely. This can be done easily:

.. code-block:: python

   await sleeper.run(3)

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
   TaskResult(fn_name='sleeper', enqueue_time=1740763800091, success=True, result=3, start_time=1740763805099, finish_time=1740763808102)
   TaskStatus.DONE

The ``TaskResult`` object contains information about the task, such as start/end time. The ``success`` flag will tell you whether the object stored in ``result`` is the result of task execution (if ``True``) or an exception raised during execution (if ``False``).

Task context
------------

As we've already seen, tasks can access the worker context via ``Worker.context`` on a per-worker basis. In addition to this, streaQ provides a per-task context, ``Worker.task_context()``, with task-specific information such as the try count:

.. code-block:: python

   @worker.task()
   async def get_id() -> str:
       ctx = worker.task_context()
       return ctx.task_id

Calls to ``Worker.task_context()`` anywhere outside of a task or a middleware will result in an error.

Retrying tasks
--------------

streaQ provides a special exception that you can raise manually inside of your tasks to make sure that they're retried (as long as ``tries <= max_tries`` for that task):

.. code-block:: python

   from streaq.task import StreaqRetry

   @worker.task()
   async def try_thrice() -> bool:
       if worker.task_context().tries < 3:
           raise StreaqRetry("Retrying!")
       return True

By default, the retries will use an exponential backoff, where each retry happens after a ``try**2`` second delay. To change this behavior, you can pass the ``delay`` parameter to the ``StreaqRetry`` exception.

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
   async def cron() -> None:
       print("Itsa me, Mario!")

The ``cron`` decorator has one required parameter, the crontab to use which follows the format specified `here <https://github.com/josiahcarlson/parse-crontab?tab=readme-ov-file#description>`_, as well as the same optional parameters as the ``task`` decorator.

The timezone used for the scheduler can be controlled via the worker's ``tz`` parameter.

Synchronous functions
---------------------

streaQ also supports synchronous functions as second-class citizens for use with mixed codebases. Sync functions will be run in a separate thread, so they won't block the event loop.

Note that if the task waiting for its completion is cancelled, the thread will still run its course but its return value (or any raised exception) will be ignored.

.. code-block:: python

   import time

   @worker.task()
   def sync_sleep(seconds: int) -> int:
       time.sleep(seconds)
       return seconds

   async with worker:
      task = await sync_sleep.enqueue(1)
      print(await task.result(3))

Task dependency graph
---------------------

streaQ supports chaining tasks together in a dependency graph. This means that tasks depending on other tasks won't be enqueued until their dependencies have finished successfully. If the dependency fails, the dependent task will fail as well.

Dependencies can be specified using the ``after`` parameter of the ``Task.start`` function:

.. code-block:: python

   async with worker:
       task1 = await sleeper.enqueue(1)
       task2 = await sleeper.enqueue(2).start(after=task1.id)
       task3 = await sleeper.enqueue(3).start(after=[task1.id, task2.id])

And the dependency failing will cause dependent tasks to fail as well:

.. code-block:: python

    @worker.task()
    async def foobar() -> None:
        raise Exception("Oh no!")

    @worker.task()
    async def do_nothing() -> None:
        pass

    async with worker:
        task = await foobar.enqueue().start()
        dep = await do_nothing.enqueue().start(after=task.id)
        print(await dep.result(3))

Task pipelining
---------------

streaQ also supports task pipelining via the dependency graph, allowing you to directly feed the results of one task to another. Let's build on the ``fetch`` task defined earlier:

.. code-block:: python

   @worker.task(timeout=5)
   async def fetch(url: str) -> int:
       res = await worker.context.http_client.get(url)
       return len(res.text)

   @worker.task()
   async def double(val: int) -> int:
       return val * 2

   @worker.task()
   async def is_even(val: int) -> bool:
       return val % 2 == 0

   async with worker:
       task = await fetch.enqueue("https://tastyware.dev").then(double).then(is_even)
       print(await task.result(3))

.. code-block:: python

   TaskResult(fn_name='is_even', enqueue_time=1743469913601, success=True, result=True, start_time=1743469913901, finish_time=1743469913902)

This is useful for ETL pipelines or similar tasks, where each task builds upon the result of the previous one. With a little work, you can build common pipelining utilities from these building blocks:

.. code-block:: python

   from typing import Any, Sequence
   from streaq.utils import to_tuple

   @worker.task()
   async def map(data: Sequence[Any], to: str) -> list[Any]:
       task = worker.registry[to]
       coros = [task.enqueue(*to_tuple(d)).start() for d in data]
       tasks = await asyncio.gather(*coros)
       results = await asyncio.gather(*[t.result(3) for t in tasks])
       return [r.result for r in results]

   @worker.task()
   async def filter(data: Sequence[Any], by: str) -> list[Any]:
       task = worker.registry[by]
       coros = [task.enqueue(*to_tuple(d)).start() for d in data]
       tasks = await asyncio.gather(*coros)
       results = await asyncio.gather(*[t.result(5) for t in tasks])
       return [data[i] for i in range(len(data)) if results[i].result]

   async with worker:
       data = [0, 1, 2, 3]
       t1 = await map.enqueue(data, to=double.fn_name).then(filter, by=is_even.fn_name)
       print(await t1.result())
       t2 = await filter.enqueue(data, by=is_even.fn_name).then(map, to=double.fn_name)
       print(await t2.result())

.. code-block:: python

   TaskResult(fn_name='filter', enqueue_time=1751712228859, success=True, result=[0, 2, 4, 6], start_time=1751712228895, finish_time=1751712228919)
   TaskResult(fn_name='map', enqueue_time=1751712228923, success=True, result=[0, 4], start_time=1751712228951, finish_time=1751712228966)

.. note::
   For pipelined tasks, positional arguments must all come from the previous task (tuple outputs will be unpacked), and any additional arguments can be passed as kwargs to ``then()``.
