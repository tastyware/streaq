Installation
============

Via pip
-------

The easiest way to install streaQ is using pip:

::

   $ pip install streaq

Using uv is similar:

::

   $ uv add streaq

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

Use on Windows is not recommended as you lose access to signal handling, uvloop, and Make. Consider using WSL instead.
