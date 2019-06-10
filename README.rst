

.. image:: https://img.shields.io/badge/OpenTracing-enabled-blue.svg
   :target: http://opentracing.io
   :alt: OpenTracing Badge


Asyncpg Opentracing
===================

This package provides opentracing supporting for applications using `asyncpg <https://github.com/MagicStack/asyncpg>`_ library.

Insallation
-----------

.. code-block::

   $ pip install asyncpg_opentracing

Example of usage
----------------

To activate tracing of SQL commands:

1) Wrap your connection class with ``tracing_connection``\ :

.. code-block::

   from asyncpg.connection import Connection
   from asyncpg_opentracing import tracing_connection

   @tracing_connection
   class MyConnection(Connection):
       pass

2) Pass it as ``connection_class`` to the ``connect`` function:

.. code-block::

   await asyncpg.connect(user='user', password='password', database='database',
                         connection_class=MyConnection)

or to the ``create_pool`` function:

.. code-block::

   await asyncpg.create_pool(user='user', password='password', database='database',
                             connection_class=MyConnection)
