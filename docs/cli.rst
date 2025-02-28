Command-line interface
======================

Assuming you have a file called ``example.py`` which defines an instance of ``streaq.Worker`` called ``worker``, you can run a worker process like so:

.. code-block::

   $ streaq example.worker

You can always run ``streaq -h`` to see the help page:

.. code-block:: text

   Usage: streaq [OPTIONS] WORKER_PATH

   Arguments:
     WORKER_PATH  [required]

   Options:
     --workers INTEGER               Number of worker processes to spin up
                                     [default: 1]
     -b, --burst                     Whether to shut down worker when the queue
                                     is empty
     -v, --verbose                   Whether to use logging.DEBUG instead of
                                     logging.INFO
     --version                       Show installed version
     -w, --watch                     Whether to reload the worker upon changes
                                     detected
     --install-completion [bash|zsh|fish|powershell|pwsh]
                                     Install completion for the specified shell.
     --show-completion [bash|zsh|fish|powershell|pwsh]
                                     Show completion for the specified shell, to
                                     copy it or customize the installation.
     -h, --help                      Show this message and exit.
