Installation
============

Via pypi
--------

The easiest way to install streaQ is using pip:

::

   $ pip install streaq

From source
-----------

You can also install from source.
Make sure you have `uv <https://docs.astral.sh/uv/getting-started/installation/>`_ installed beforehand.

::

   $ git clone https://github.com/tastyware/streaq.git
   $ cd streaq
   $ make install

If you're contributing, you'll want to run tests on your changes locally:

::

   $ make lint
   $ make test

If you want to build the documentation (usually not necessary):

::

   $ make docs

Windows
-------

If you want to install from source on Windows, you can't use the Makefile, so just run the commands individually. For example:

::

   $ git clone https://github.com/tastyware/streaq.git
   $ cd streaq
   $ uv sync
   $ uv pip install -e .
