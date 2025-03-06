Command-line interface
======================

Assuming you have a file called ``example.py`` which defines an instance of ``streaq.Worker`` called ``worker``, you can run a worker process like so:

.. code-block::

   $ streaq example.worker

You can always run ``streaq --help`` to see the help page:

.. code-block:: text

   Usage: streaq [OPTIONS] WORKER_PATH

   ╭─ Arguments ────────────────────────────────────────────────────────────────────────────────────────────╮
   │ *    worker_path      TEXT  [default: None] [required]                                                 │
   ╰────────────────────────────────────────────────────────────────────────────────────────────────────────╯
   ╭─ Options ──────────────────────────────────────────────────────────────────────────────────────────────╮
   │ --workers             -w      INTEGER  Number of worker processes to spin up [default: 1]              │
   │ --burst               -b               Whether to shut down worker when the queue is empty             │
   │ --host                -h      TEXT     Host for web UI process [default: 127.0.0.1]                    │
   │ --port                -p      INTEGER  Port for web UI process [default: 8001]                         │
   │ --reload              -r               Whether to reload the worker upon changes detected              │
   │ --verbose             -v               Whether to use logging.DEBUG instead of logging.INFO            │
   │ --version                              Show installed version                                          │
   │ --web                                  Whether to run web UI to monitor tasks                          │
   │ --install-completion                   Install completion for the current shell.                       │
   │ --show-completion                      Show completion for the current shell, to copy it or customize  │
   │                                        the installation.                                               │
   │ --help                                 Show this message and exit.                                     │
   ╰────────────────────────────────────────────────────────────────────────────────────────────────────────╯
