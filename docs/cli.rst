Command-line interface
======================

Assuming you have a file called ``example.py`` which defines an instance of ``streaq.Worker`` called ``worker``, you can run a worker process like so:

.. code-block::

   $ streaq run example:worker

You can always run ``streaq --help`` to see the help page:

.. code-block:: text

    Usage: streaq [OPTIONS] COMMAND [ARGS]...

   ╭─ Options ──────────────────────────────────────────────────────────────────────────────────────────────╮
   │ --version                     Show installed version                                                   │
   │ --install-completion          Install completion for the current shell.                                │
   │ --show-completion             Show completion for the current shell, to copy it or customize the       │
   │                               installation.                                                            │
   │ --help                        Show this message and exit.                                              │
   ╰────────────────────────────────────────────────────────────────────────────────────────────────────────╯
   ╭─ Commands ─────────────────────────────────────────────────────────────────────────────────────────────╮
   │ run   Run one or more workers with the given options                                                   │
   │ web   Run a web UI for monitoring with the given options                                               │
   ╰────────────────────────────────────────────────────────────────────────────────────────────────────────╯

There's also a helpful web UI for monitoring which you can run with:

.. code-block::

   $ streaq web example:worker
