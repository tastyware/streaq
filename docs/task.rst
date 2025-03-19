Tasks
=====

Task execution
--------------

streaQ preserves arq's task execution model, called "pessimistic execution": tasks aren’t removed from the queue until they’ve either succeeded or failed. If the worker shuts down, the task will be cancelled immediately and will remain in the queue to be run again when the worker starts up again (or gets run by another worker which is still running).

All streaQ tasks should therefore be designed to cope with being called repeatedly if they’re cancelled. If necessary, use database transactions, idempotency keys or Redis to mark when non-repeatable work has completed to avoid making it twice.

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

Tasks can depend on other tasks, meaning they won't be enqueued until their dependencies have finished successfully. If the dependency fails, the dependent task will not be enqueued.

.. code-block:: python

   async with worker:
       task1 = await sleeper.enqueue(1)
       task2 = await sleeper.enqueue(2).start(after=task1.id)
       task3 = await sleeper.enqueue(3).start(after=[task1.id, task2.id])

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
    async def foobar(ctx: WrappedContext[None]) -> None:
        raise Exception("Oh no!")

    @worker.task()
    async def do_nothing(ctx: WrappedContext[None]) -> None:
        pass

    async with worker:
        task = await foobar.enqueue().start()
        dep = await do_nothing.enqueue().start(after=task.id)
        print(await dep.result(3))

Task priorities
---------------

Sometimes, certain critical tasks should "skip the line" and receive priority over other tasks. streaQ supports this by allowing you to specify a priority when enqueuing tasks. If a low priority queue is backed up, you can use a high priority queue to ensure that critical tasks are executed quickly.

There are three priorities: ``TaskPriority.LOW``, ``TaskPriority.MEDIUM``, and ``TaskPriority.HIGH``. By default, tasks are enqueued with a priority of ``TaskPriority.LOW``. You can specify a priority like so:

.. code-block:: python

   from streaq import TaskPriority

   async with worker:
       await sleeper.enqueue(3).start(priority=TaskPriority.HIGH)

Here's an example that demonstrates how priorities work. Note that the low priority task is enqueued first, but the high priority task is executed first. (Make sure to run this before starting the worker!)

.. code-block:: python

   worker = Worker(concurrency=1)  # max 1 task running at a time for demo

   @worker.task()
   async def low(ctx: WrappedContext[None]) -> None:
       print("Low priority task")

   @worker.task()
   async def high(ctx: WrappedContext[None]) -> None:
       print("High priority task")

   async with worker:
       await low.enqueue().start(priority=TaskPriority.LOW)
       await high.enqueue().start(priority=TaskPriority.HIGH)

.. note::
   Priorities can only be configured for tasks that are being enqueued directly. Tasks that are scheduled or deferred (and cron jobs) will always be enqueued with a priority of ``TaskPriority.MEDIUM``, which helps ensure they are executed close to their scheduled time.
