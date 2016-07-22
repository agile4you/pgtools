# -*- coding: utf-8 -*-
"""`pgtools.pool` module.

Provides a gevent powered Postgresql connection pool
"""

from __future__ import absolute_import


__date__ = '2016-1-22'
__version__ = '1.1'
__all__ = ('PostgresPool', )

import six
import contextlib
import json
import gevent
from collections import OrderedDict
from gevent.queue import Queue
import gevent.socket as g_socket
from psycopg2 import (extensions, OperationalError, connect)
import psycopg2.extras
import sys


wait_read = getattr(g_socket, 'wait_read')
wait_write = getattr(g_socket, 'wait_write')

integer_types = six.integer_types


class DBPoolError(Exception):
    """Raises when a storage client error occurs.
    """
    pass


CURSOR_FETCH = (
    ("many", "fetchall"),
    ("single", "fetchone")
)


def jsonb_loads(stream):
    return json.loads(stream, object_pairs_hook=OrderedDict)


set_callback = getattr(extensions, 'set_wait_callback')


psycopg2.extras.register_default_json(globally=True, loads=jsonb_loads)


def gevent_wait_callback(conn, timeout=None):
    """"A wait callback to allow gevent to work with Psycopg2.
    (See docs for `psycopg2` async operations.)

    Args:
        conn (instance): A `psycopg2.connection` instance.
        timeout(integer): The pooling timeout seconds.

    Raises:
        OperationalError, for invalid pooling state.
    """
    while 1:
        state = conn.poll()
        if state == extensions.POLL_OK:
            break
        elif state == extensions.POLL_READ:
            wait_read(conn.fileno(), timeout=timeout)
        elif state == extensions.POLL_WRITE:
            wait_write(conn.fileno(), timeout=timeout)
        else:
            raise OperationalError(
                "Bad result from poll: %r" % state)


set_callback(gevent_wait_callback)


class ClientPool(object):
    """Base Interface for Gevent-coroutine based DBAPI2 connection pooling.

    Implementation uses `gevent` Queueing mechanism so we can ensure that
    a DB tasks will be not be claimed from more that one Greenlet.


    Attributes:
        maxsize (int): Greenlet pool size.
    """

    def __init__(self, maxsize=20):
        if not isinstance(maxsize, integer_types):
            raise TypeError('Expected integer, got %r' % (maxsize, ))
        self.maxsize = maxsize
        self.pool = Queue()
        self.size = 0

    def create_connection(self):
        raise NotImplemented("Must implement `create_connection` method.")

    def get(self):
        pool = self.pool
        if self.size >= self.maxsize or pool.qsize():
            return pool.get()
        else:
            self.size += 1
            try:
                new_item = self.create_connection()
            except:
                self.size -= 1
                raise
            return new_item

    def put(self, item):
        self.pool.put(item)

    def closeall(self):
        while not self.pool.empty():
            conn = self.pool.get_nowait()
            try:
                conn.close()
            except Exception:
                pass

    @contextlib.contextmanager
    def connection(self, isolation_level=None):
        conn = self.get()
        try:
            if isolation_level is not None:
                if conn.isolation_level == isolation_level:
                    isolation_level = None
                else:
                    conn.set_isolation_level(isolation_level)
            yield conn
        except:
            if conn.closed:
                conn = None
                self.closeall()
            else:
                conn = self._rollback(conn)
            raise
        else:
            if conn.closed:
                raise OperationalError(
                    "Cannot commit because connection was closed: %r" % conn
                )
            conn.commit()
        finally:
            if conn is not None and not conn.closed:
                if isolation_level is not None:
                    conn.set_isolation_level(isolation_level)
                self.put(conn)

    @contextlib.contextmanager
    def cursor(self, *args, **kwargs):
        isolation_level = kwargs.pop('isolation_level', None)
        with self.connection(isolation_level) as conn:
            yield conn.cursor(*args, **kwargs)

    def _rollback(self, conn):
        try:
            conn.rollback()
        except:
            gevent.get_hub().handle_error(conn, *sys.exc_info())
            return
        return conn

    def execute(self, *args, **kwargs):
        with self.cursor(**kwargs) as cursor:
            cursor.execute(*args)
            return cursor.rowcount

    def fetchone(self, *args, **kwargs):
        with self.cursor(**kwargs) as cursor:
            cursor.execute(*args)
            return cursor.fetchone()

    def fetchall(self, *args, **kwargs):
        with self.cursor(**kwargs) as cursor:
            cursor.execute(*args)
            return cursor.fetchall()

    def fetchiter(self, *args, **kwargs):
        with self.cursor(**kwargs) as cursor:
            cursor.execute(*args)
            while True:
                items = cursor.fetchmany()
                if not items:
                    break
                for item in items:
                    yield item

    def query(self, query, fetch_opts='many', cursor_type='RealDictCursor'):
        try:
            return getattr(self, dict(CURSOR_FETCH).get(fetch_opts))(
                *(query, ),
                cursor_factory=getattr(psycopg2.extras, cursor_type)
            )
        except Exception as e:
            raise DBPoolError(e.args)


class PostgresPool(ClientPool):
    """Postgresql Server connection Pooling class.
    """

    def __init__(self, *args, **kwargs):
        self.connect = kwargs.pop('connect', connect)
        maxsize = kwargs.pop('maxsize', 30)
        self.args = args
        self.kwargs = kwargs
        ClientPool.__init__(self, maxsize)

    def create_connection(self):
        return self.connect(*self.args, **self.kwargs)
