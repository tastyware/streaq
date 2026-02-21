Tasks
=====

Task execution
--------------

streaQ uses a task execution model called "pessimistic execution" or "at-least-once": tasks aren’t removed from the queue until they’ve either succeeded or failed. If the worker shuts down, the task will remain in the queue to be picked up by another worker. ``Worker.idle_timeout`` controls how often task liveness is updated (and consequently, how quickly stuck tasks can be retried).

All streaQ tasks should therefore be designed to cope with being called repeatedly if they’re cancelled. If necessary, use database transactions, idempotency keys or Redis to mark when non-repeatable work has completed to avoid doing it twice. Alternatively, you can opt-out of this behavior on a per-task basis by passing ``max_tries=1`` to the task constructor.

.. note::
   Idempotency is super easy with Redis, see `here <https://gist.github.com/Graeme22/5cd3bffba46480d3936dad407b14d6a4>`_!

streaQ handles exceptions in the following manner:

* ``StreaqRetry`` exceptions result in retrying the task, sometimes after a delay (see below).
* ``asyncio.CancelledError`` or ``trio.Cancelled`` exceptions result in the task failing if the task was aborted by the user, or being retried if the worker was shut down unexpectedly.
* Any other exception will result in the task failing.

Registering tasks
-----------------

In order to run tasks, they must first be registered with the worker. Let's assume we have a worker that looks like this:

.. code-block:: python

   from streaq import Worker
   worker = Worker(redis_url="redis://localhost:6379")

We can now register async functions with the worker:

.. code-block:: python

   from anyio import sleep  # you can just as well use asyncio or trio

   @worker.task
   async def sleeper(time: int) -> int:
       await sleep(time)
       return time

The ``task`` decorator has several optional arguments that can be used to customize behavior:

- ``expire``: time after which to dequeue the task, if ``None`` will never be dequeued
- ``max_tries``: maximum number of attempts before giving up if task is retried; defaults to ``3``
- ``name``: use a custom name for the task instead of the function name
- ``silent``: whether to silence task logs; defaults to False
- ``timeout``: amount of time to run the task before raising ``TimeoutError``; ``None`` (the default) means never timeout
- ``ttl``: amount of time to store task result in Redis; defaults to 5 minutes. ``None`` means never delete results, ``0`` means never store results
- ``unique``: whether to prevent more than one instance of the task running simultaneously; defaults to ``False`` for normal tasks and ``True`` for cron jobs. (Note that more than one instance may be queued, but two running at once will cause the second to fail.)

For example:

.. code-block:: python

   @worker.task(timeout=3, max_tries=1)
   async def foo(): ...

Enqueuing tasks
---------------

Once registered, tasks can then be queued up for execution by worker processes (with full type safety!) using the worker's async context manager:

.. code-block:: python

   async with worker:
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

.. note::
   ``Task.enqueue()`` is actually a sync function that returns a ``Task`` object. Since ``Task`` is awaitable, it gets enqueued when awaited. Therefore, you should always use await even though ``Task.enqueue()`` is sync, unless you're enqueuing by batch (see below).

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

   @worker.task
   async def low() -> None:
       print("Low priority task")

   @worker.task
   async def high() -> None:
       print("High priority task")

   async with worker:
       await low.enqueue().start(priority="low")
       await high.enqueue().start(priority="high")

Enqueuing by batch
------------------

For most cases, the above method of enqueuing tasks is sufficient. However, streaQ also provides a way to enqueue a group of tasks together in order to maximize efficiency:

.. code-block:: python

   # importantly, we're not using `await` here
   tasks = [sleeper.enqueue(i) for i in range(10)]
   async with worker:
       await worker.enqueue_many(tasks)

Running tasks locally
---------------------

Sometimes, you may wish to run a task's underlying function directly and skip enqueuing entirely. This can be done easily:

.. code-block:: python

   await sleeper(3)

Note that tasks that require access to ``WorkerDepends`` or ``TaskDepends`` will fail when run this way as context is handled by running workers.

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
   TaskResult(fn_name='sleeper', enqueue_time=1740763800091, success=True, start_time=1740763805099, finish_time=1740763808102, tries=1, worker_id='ca5bd9eb', _result=3)
   TaskStatus.DONE

The ``TaskResult`` object contains information about the task, such as start/end time. The ``TaskResult.success`` flag will tell you whether the task succeeded or failed. If ``True``, you can access the result via ``TaskResult.result``; otherwise, you can access the exception via ``TaskResult.exception``.

Task exceptions
---------------

If an exception occurs while performing the task, the ``TaskResult.success`` flag will be set to ``False`` and the exception object will be available in the ``TaskResult.exception`` property:

.. code-block:: python

    async with worker:
        result = await task.result()

        if not result.success:
            print(result.exception)

.. important::

    If you're using the default serialization (pickle), the exception object won't contain traceback information, since pickle doesn't natively support serializing traceback objects — this information will be lost during serialization and deserialization.

    To keep the full traceback details for exceptions, you can use the `python-tblib <https://github.com/ionelmc/python-tblib>`_ package. This package makes it easy to serialize traceback objects with pickle. In most cases just two lines of code are needed to add this support:

    .. code-block:: python

        from tblib import pickling_support

        # Declare your custom exceptions
        class MyException(Exception): ...

        # Finally, install tblib
        pickling_support.install()


Task context
------------

As we've already seen, tasks can access the worker context via ``WorkerDepends``. In addition to this, streaQ provides a per-task context via ``TaskDepends``, with task-specific information such as the try count:

.. code-block:: python

   from streaq import TaskContext, TaskDepends

   @worker.task
   async def get_id(ctx: TaskContext = TaskDepends()) -> str:
       return ctx.task_id

Calls to ``TaskDepends()`` anywhere outside of a task or a middleware will result in an error.

Retrying tasks
--------------

streaQ provides a special exception that you can raise manually inside of your tasks to make sure that they're retried (as long as ``tries <= max_tries`` for that task):

.. code-block:: python

   from streaq import StreaqRetry

   @worker.task
   async def try_thrice(ctx: TaskContext = TaskDepends()) -> bool:
       if ctx.tries < 3:
           raise StreaqRetry("Retrying!")
       return True

By default, the retries will use an exponential backoff, where each retry happens after a ``try**2`` second delay. To change this behavior, you can pass the ``delay`` or ``schedule`` parameters to the ``StreaqRetry`` exception.

Cancelling tasks
----------------

Tasks that are running or enqueued can be aborted manually:

.. code-block:: python

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
       print("Kyrie eleison!")

The ``cron`` decorator has one required parameter: the crontab to use, which follows the format specified `here <https://github.com/josiahcarlson/parse-crontab?tab=readme-ov-file#description>`_. It also has the same optional parameters as the ``task`` decorator.

The timezone used for the scheduler can be controlled via the worker's ``tz`` parameter.

Dynamic cron jobs
-----------------

Aside from defining cron jobs with the decorator, you can also schedule tasks dynamically:

.. code-block:: python

   task = await sleeper.enqueue(1).start(schedule="*/5 * * * *")  # every 5 minutes

This causes the task to run repeatedly with the given arguments at the given schedule. To stop scheduling a repeating task, you can use:

.. code-block:: python

   await task.unschedule()
   # OR
   await worker.unschedule_by_id(task.id)

Synchronous functions
---------------------

streaQ also supports synchronous functions as second-class citizens for use with mixed codebases. Sync functions will be run in a separate thread, so they won't block the event loop.

Note that if the task waiting for its completion is cancelled, the thread will still run its course but its return value (or any raised exception) will be ignored.

.. code-block:: python

   import time

   @worker.task
   def sync_sleep(seconds: int) -> int:
       time.sleep(seconds)
       return seconds

   # here we use await, the wrapper does the magic for us!
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

    @worker.task
    async def foobar() -> None:
        raise Exception("Oh no!")

    @worker.task
    async def do_nothing() -> None:
        pass

    async with worker:
        task = await foobar.enqueue().start()
        dep = await do_nothing.enqueue().start(after=task.id)
        print(await dep.result(3))

Task pipelining
---------------

streaQ also supports task pipelining via the dependency graph, allowing you to directly feed the results of one task to another while maintaining type safety. Let's build on the ``fetch`` task defined earlier:

.. code-block:: python

   @worker.task(timeout=5)
   async def fetch(url: str, ctx: WorkerContext = WorkerDepends()) -> int:
       res = await ctx.http_client.get(url)
       return len(res.text)

   @worker.task
   async def double(val: int) -> int:
       return val * 2

   @worker.task
   async def is_even(val: int) -> bool:
       return val % 2 == 0

   async with worker:
       task = await fetch.enqueue("https://tastyware.dev").then(double).then(is_even)
       print(await task.result(3))

.. code-block:: python

   TaskResult(fn_name='is_even', enqueue_time=1743469913601, success=True, start_time=1743469913901, finish_time=1743469913902, tries=1, worker_id='ca5bd9eb', _result=True)

This is useful for ETL pipelines or similar tasks, where each task builds upon the result of the previous one. With a little work, you can build common pipelining utilities from these building blocks:

.. code-block:: python

   from typing import Any
   from streaq.utils import gather, to_tuple

   @worker.task
   async def map(data: list[Any], *, to: str) -> list[Any]:
       task = worker.registry[to]
       coros = [task.enqueue(*to_tuple(d)).start() for d in data]
       tasks = await gather(*coros)
       results = await gather(*[t.result(3) for t in tasks])
       return [r.result for r in results]

   @worker.task
   async def filter(data: list[Any], *, by: str) -> list[Any]:
       task = worker.registry[by]
       coros = [task.enqueue(*to_tuple(d)).start() for d in data]
       tasks = await gather(*coros)
       results = await gather(*[t.result(5) for t in tasks])
       return [data[i] for i in range(len(data)) if results[i].result]

   async with worker:
       data = [0, 1, 2, 3]
       t1 = await map.enqueue(data, to=double.fn_name).then(filter, by=is_even.fn_name)
       print(await t1.result())
       t2 = await filter.enqueue(data, by=is_even.fn_name).then(map, to=double.fn_name)
       print(await t2.result())

.. code-block:: python

   TaskResult(fn_name='filter', enqueue_time=1751712228859, success=True, start_time=1751712228895, finish_time=1751712228919, tries=1, worker_id='ca5bd9eb', _result=[0, 2, 4, 6])
   TaskResult(fn_name='map', enqueue_time=1751712228923, success=True, start_time=1751712228951, finish_time=1751712228966, tries=1, worker_id='ca5bd9eb', _result=[0, 4])

.. warning::
   For pipelined tasks, positional arguments must all come from the previous task (tuple outputs will be unpacked), and any additional arguments can be passed as kwargs to ``then()``.

   Here's an example that takes advantage of this behavior:

   .. code-block:: python

      @worker.task
      async def tuplify(input: int) -> tuple[int, int]:
          return (input, input)

      @worker.task
      async def untuple(first: int, second: int, *, third: int = 0) -> int:
          return first + second + third

      async with worker:
          task = await tuplify.enqueue(3).then(untuple, third=3)
          res = await task.result(3)
          print(res.result)  # 9

If you don't need to pass additional arguments, tasks can be pipelined using the ``|`` operator as a convenience:

.. code-block:: python

   async with worker:
       await (fetch.enqueue("https://tastyware.dev") | double | is_even)

Retrieving Tasks
----------------

You may need to interact with tasks in another context than the one in which you enqueued them first. Thus you need to retrieve tasks from Redis and get their context.

You can use ``get_tasks_by_status()`` function to do so.

.. code-block:: python

  from streaq import Worker
  from streaq.task import TaskStatus

  worker = Worker()

  # Returns list[TaskInfo] for SCHEDULED, QUEUED, RUNNING
  await worker.get_tasks_by_status(TaskStatus.SCHEDULED)
  await worker.get_tasks_by_status(TaskStatus.QUEUED)
  await worker.get_tasks_by_status(TaskStatus.RUNNING)

  # Returns list[TaskResult] for DONE
  await worker.get_tasks_by_status(TaskStatus.DONE)

From this point, you can retrieve the given task you expect and do whatever you want with it, from accessing its state, its result or cancelling it.
