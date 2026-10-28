Pipelining
==========

Basic pipelining
----------------

streaQ supports task pipelining, allowing you to directly feed the results of one task to another while maintaining type safety. Let's build on the ``fetch`` task defined earlier:

.. code-block:: python

   @worker.task(timeout=5)
   async def fetch(url: str) -> int:
       res = await worker.context.http_client.get(url)
       return len(res.text)

   @worker.task
   async def double(val: int) -> int:
       return val * 2

   @worker.task
   async def is_even(val: int) -> bool:
       return val % 2 == 0

   async with worker:
       task = await fetch.enqueue("https://tastyware.dev").then(double)().then(is_even)()
       print(await task.result(3))

.. code-block:: python

   TaskResult(fn_name='is_even', enqueue_time=1743469913601, success=True, start_time=1743469913901, finish_time=1743469913902, tries=1, worker_id='ca5bd9eb', _result=True)

This is useful for ETL pipelines or similar tasks, where each task builds upon the result of the previous one. streaQ also packages some common pipelining utilities building on this foundation:

.. code-block:: python

   async with worker:
       data = [0, 1, 2, 3]
       t1 = await double.map(data)().filter(is_even)()
       print((await t1.result()).result)  # [0, 2, 4, 6]
       t2 = await is_even.filter(data)().map(double)()
       print((await t2.result()).result)  # [0, 4]

Notice that pipeline utilities are `curried <https://en.wikipedia.org/wiki/Currying>`_. This is because, although the first argument comes from the preceding task, any additional arguments should be provided at enqueue time:

.. code-block:: python

   @worker.task
   async def is_divisible(val: int, by: int) -> bool:
       return val % by == 0

   async with worker:
       task = await double.enqueue(3).then(is_divisible)(4)

In some cases you might not want to pass the result of the parent task at all. This can be done by passing ``forward=False``:

.. code-block:: python

   async with worker:
       await do_work.enqueue(some_args).then(notify, forward=False)(other_args)

Here, ``notify`` won't run until ``do_work`` finishes, but it won't receive a forwarded result.

Fail-over tasks
---------------

Similar to ``Task.then()``, streaQ also provides ``Task.otherwise()``, which lets a backup task replay its parent's arguments if a task fails:

.. code-block:: python

   @worker.task
   async def fails(val: int) -> int:
       raise Exception("Oh no!")

   @worker.task
   async def double(val: int) -> int:
       return val * 2

   async with worker:
       task = await fails.enqueue(3).otherwise(double)()
       print((await task.result(3)).result)  # 6

Both tasks must have the same function signature (parameters and return type) in order to use fail-overs. Here ``task.result()`` will be the result of ``fails(3)`` if it succeeds, otherwise the result of ``double(3)``.

.. _chains-and-the-dlq:

Chains and the dead-letter queue
--------------------------------

When the :ref:`dead-letter queue <dead-letter-queue>` is enabled, a parked task leaves its chain untouched. The chain then resumes if the parked task is requeued and runs successfully. If the task is purged or swept after ``dlq_ttl``, the chain will then fail normally.

Meanwhile, ``result()`` on any later task in the chain keeps waiting. With ``dlq_ttl=None``, that's until you requeue or purge by hand. A task followed by an ``otherwise()`` fallback isn't parked, since the fallback runs immediately instead.

Complex workflows
-----------------

``then()``, ``otherwise()``, ``map()``, and ``filter()`` cover the common pipeline shapes. For anything else, such as diamonds, conditional steps, loops, or joins of multi-step branches, you can write your own *orchestrator* task: an ordinary task that enqueues other tasks and waits for their results.

To make retries safe if an orchestrator fails, times out, or a worker dies, you can make use of streaQ's idempotency guarantees and give each task it enqueues an ID *derived from the orchestrator's own* with ``TaskContext.derive_id()``.

For example, here's a "diamond" workflow shape: a page is fetched, analyzed two different ways in parallel, then the results are merged.

.. code-block:: text

        fetch
       /     \
   parse    extract_links
       \     /
        merge
          |
         ...

.. code-block:: python
   :emphasize-lines: 19,22,23-25,28-30

   from anyio import gather

   @worker.task
   async def fetch(url: str) -> str: ...

   @worker.task
   async def parse(html: str, strict: bool = False) -> dict[str, str]: ...

   @worker.task
   async def extract_links(html: str, limit: int) -> list[str]: ...

   @worker.task
   async def merge(parsed: dict[str, str], links: list[str]) -> Report: ...

   @worker.task
   async def analyze(url: str) -> Report:
       ctx = analyze.context
       # top of the diamond
       page = await fetch.enqueue(url).start(id=ctx.derive_id(key="fetch"))
       html = (await page.result()).result
       # both branches run in parallel, each with its own arguments
       parsed = await parse.enqueue(html, strict=True).start(id=ctx.derive_id(key="parse"))
       links = await extract_links.enqueue(html, limit=50).start(
           id=ctx.derive_id(key="links")
       )
       parsed_res, links_res = await gather(parsed.result(), links.result())
       # bottom of the diamond
       report = await merge.enqueue(parsed_res.result, links_res.result).start(
           id=ctx.derive_id(key="merge")
       )
       return (await report.result()).result

   async with worker:
       task = await analyze.enqueue("https://tastyware.dev")

On a retry, ``derive_id`` returns the same IDs, so tasks that already ran are deduplicated, and ``result()`` returns their stored results instead of running them again. Only the work that hadn't finished is redone.

Rules for orchestration
~~~~~~~~~~~~~~~~~~~~~~~

- Keys must be unique within the orchestrator.
- After a nested failure, requeue the failed subtask **first** (see :ref:`dead-letter-queue`), *then* the orchestrator. Otherwise the orchestrator reuses the subtask's stored failure.

Orchestrators hold a worker slot
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

An orchestrator occupies one of its worker's concurrency slots while it waits, so make sure concurrency is high enough the orchestrator doesn't get blocked, or run orchestrators on dedicated workers with a separate queue.

For long-running tasks you don't want to hold slots indefinitely, you can poll instead:

.. code-block:: python

   if not await is_approved(order_id):
       raise StreaqRetry(delay=300)  # register the task with max_tries=None

Waiting on many tasks
~~~~~~~~~~~~~~~~~~~~~

Each call to ``Task.result()`` opens its own pub/sub connection while it waits. That's fine for a handful of tasks, but an orchestrator fanning out to hundreds of tasks would hold hundreds of connections. ``Worker.results_for()`` waits for any number of tasks over a single connection:

.. code-block:: python

   @worker.task
   async def crawl(urls: list[str]) -> list[str]:
       ctx = crawl.context
       tasks = [
           fetch.enqueue(url).start(id=ctx.derive_id(key=url)).then(parse)()
           for url in urls
       ]
       await worker.enqueue_many(tasks)
       results = await worker.results_for(tasks)
       return [res.result["title"] for res in results]
