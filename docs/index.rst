.. image:: https://readthedocs.org/projects/streaq/badge/?version=latest
   :target: https://streaq.readthedocs.io/en/latest/?badge=latest
   :alt: Documentation Status

.. image:: https://img.shields.io/pypi/v/streaq
   :target: https://pypi.org/project/streaq
   :alt: PyPI Package

.. image:: https://static.pepy.tech/badge/streaq
   :target: https://pepy.tech/project/streaq
   :alt: PyPI Downloads

.. image:: https://img.shields.io/github/v/release/tastyware/streaq?label=release%20notes
   :target: https://github.com/tastyware/streaq/releases
   :alt: Release

streaQ documentation
====================

Fast, async, type-safe job queuing with Redis streams

.. tip::
   Sick of ``redis-py``? Check out `coredis <https://coredis.readthedocs.io/en/latest/>`_, a fully-typed Redis client that supports Trio!

Feature comparison
------------------

+----------------------------+--------+-----+-----+--------+
| Library                    | taskiq | arq | SAQ | streaQ |
+============================+========+=====+=====+========+
| Startup/shutdown hooks     | ✅     | ✅  | ✅  | ✅     |
+----------------------------+--------+-----+-----+--------+
| Task scheduling/cron jobs  | ✅     | ✅  | ✅  | ✅     |
+----------------------------+--------+-----+-----+--------+
| Task middleware            | ✅     | ✅  | ✅  | ✅     |
+----------------------------+--------+-----+-----+--------+
| Web UI available           | ✅     | ✅  | ✅  | ✅     |
+----------------------------+--------+-----+-----+--------+
| Actively maintained        | ✅     | ❌  | ✅  | ✅     |
+----------------------------+--------+-----+-----+--------+
| Custom serializers         | ✅     | ✅  | ❌  | ✅     |
+----------------------------+--------+-----+-----+--------+
| Type safe                  | ✅     | ❌  | ❌  | ✅     |
+----------------------------+--------+-----+-----+--------+
| Extensive documentation    | ✅     | ❌  | ❌  | ✅     |
+----------------------------+--------+-----+-----+--------+
| Task abortion              | ❌     | ✅  | ✅  | ✅     |
+----------------------------+--------+-----+-----+--------+
| Synchronous tasks          | ✅     | ❌  | ❌  | ✅     |
+----------------------------+--------+-----+-----+--------+
| Task dependency graph      | ✅     | ❌  | ❌  | ✅     |
+----------------------------+--------+-----+-----+--------+
| Priority queues            | ❌     | ❌  | ❌  | ✅     |
+----------------------------+--------+-----+-----+--------+
| Multiple backends          | ✅     | ❌  | ❌  | ❌     |
+----------------------------+--------+-----+-----+--------+
| Redis Sentinel support     | ✅     | ❌  | ❌  | ✅     |
+----------------------------+--------+-----+-----+--------+
| Structured concurrency     | ❌     | ❌  | ❌  | ✅     |
+----------------------------+--------+-----+-----+--------+
| Trio support               | ❌     | ❌  | ❌  | ✅     |
+----------------------------+--------+-----+-----+--------+

.. toctree::
   :maxdepth: 2
   :caption: Documentation
   :hidden:

   installation
   getting-started
   worker
   task
   middleware
   cli
   integrations
   contributing

.. toctree::
   :maxdepth: 2
   :caption: API Reference
   :hidden:

   api/task
   api/types
   api/utils
   api/worker

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
