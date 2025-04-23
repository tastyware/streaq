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

+----------------------------+--------+-----+-----+--------+
| Feature comparison         | taskiq | arq | SAQ | streaQ |
+============================+========+=====+=====+========+
| Startup/shutdown hooks     | ✅     | ✅  | ✅  | ✅     |
+----------------------------+--------+-----+-----+--------+
| Task scheduling/cron jobs  | ✅     | ✅  | ✅  | ✅     |
+----------------------------+--------+-----+-----+--------+
| Task middleware            | ✅     | ✅  | ✅  | ✅     |
+----------------------------+--------+-----+-----+--------+
| Actively maintained        | ✅     | ❌  | ✅  | ✅     |
+----------------------------+--------+-----+-----+--------+
| Custom serializers         | ✅     | ✅  | ❌  | ✅     |
+----------------------------+--------+-----+-----+--------+
| Type safe                  | ✅     | ❌  | ❌  | ✅     |
+----------------------------+--------+-----+-----+--------+
| Extensive documentation    | ✅     | ❌  | ❌  | ✅     |
+----------------------------+--------+-----+-----+--------+
| Task abortion              | ❌     | ✅  | ❌  | ✅     |
+----------------------------+--------+-----+-----+--------+
| Synchronous tasks          | ✅     | ❌  | ❌  | ✅     |
+----------------------------+--------+-----+-----+--------+
| Dependency injection       | ✅     | ❌  | ❌  | ❌     |
+----------------------------+--------+-----+-----+--------+
| Task dependency graph      | ✅     | ❌  | ❌  | ✅     |
+----------------------------+--------+-----+-----+--------+
| Priority queues            | ❌     | ❌  | ❌  | ✅     |
+----------------------------+--------+-----+-----+--------+
| Multiple backends          | ✅     | ❌  | ❌  | ❌     |
+----------------------------+--------+-----+-----+--------+
| Redis Sentinel support     | ✅     | ❌  | ❌  | ✅     |
+----------------------------+--------+-----+-----+--------+
| Web UI available           | ❌     | ✅  | ✅  | ❌     |
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
