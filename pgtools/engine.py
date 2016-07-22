# -*- coding: utf-8 -*-
"""`pgtools.pool` module.

Provides a Postgresql connection pool engine.
"""

__author__ = "Papavassiliou Vassilis"
__date__ = "2016-07-22"
__version__ = "1,2"
__all__ = ['DBPoolEngine']

import logging
import contextlib
import psycopg2
import psycopg2.pool
from psycopg2.extras import RealDictCursor


POOL_TYPE = (
    ("threaded", "ThreadedConnectionPool"),
    ("single", "SimpleConnectionPool"),
    ("persist", "PersistentConnectionPool")
)

CURSOR_FETCH = (
    ("many", "fetchall"),
    ("single", "fetchone")
)


class EngineError(Exception):
    """Base module exception.
    """
    pass


class DBPoolEngine(object):
    """Postgresql psycopg2 connection pooling class.

    `DBPool` class wraps `psycopg2` connection pooling functionality.
    It conforms to `BaseConnection` interface. Class it self operates as as a
    connection Pool Manager, as it holds information for all open connections.

    .. note::
        Supports single/multi threaded connection pooling.

    Attributes:
        - pool_size (int): Connection pool max limit.
        - pool_type (str): Connection Pool type (Threaded, Single, etc)
        - conn_data (dict): Postgresql connection kwargs.
        - debug (boolean): Indicates if database warning must be shown.
        - error_status (str): Holds traceback info of the last error occurred.


    Example is the following::

        >>> db_pool = DBPool(
        ...     pool_size=20,
        ...     pool_type="threaded",
        ...     debug=True,
        ...     host="localhost",
        ...     port=5432,
        ...     user="pav",
        ...     password="iverson",
        ...     database="benchdb"
        ... )
        ...
        >>> db_pool.query(
        ...     "INSERT INTO tmp (name) values ('ama') returning id;",
        ...     fetch_opts="single"
        ... )
        {'test_data': 1}
    """

    pool_uid = "pg://{}@{}.{}/{}"

    __slots__ = ('db', 'pool_size', 'pool_type', 'debug', 'conn_data', 'logger', 'cursor_type')

    def __init__(self, pool_size, pool_type, debug=False, cursor_type=RealDictCursor, **conn_data):
        """Initialization data.
        """
        self.db = None
        self.pool_size = pool_size
        self.pool_type = pool_type
        self.conn_data = conn_data
        self.cursor_type = cursor_type
        self.debug = debug
        self.logger = logging.getLogger(__name__)

    def __repr__(self):
        return self._pool_uid_maker(
            self.conn_data.get("user"),
            self.conn_data.get("host"),
            self.conn_data.get("port"),
            self.conn_data.get("database")
        )

    __str__ = __repr__

    def _init_connection(self):
        """Connection pool initialization.

        The main concept is to implement `lazy` connection  when we  actually
        execute a query.
        """
        self.db = self._pool_factory(self.pool_type)(
            minconn=1,
            maxconn=self.pool_size,
            **self.conn_data
        )

    @contextlib.contextmanager
    def _get_cursor(self):
        """Returns an active connection from persistence pool as
        context manager and returns the connection back to pool when finishes.

        :param cursor_type (str):
        :return: Context manager instance.

        """
        if not self.db or self.db.closed:
            self._init_connection()

        connection = self.db.getconn()
        connection.autocommit = True

        try:
            yield connection.cursor(
                cursor_factory=self.cursor_type
            )
            connection.commit()
        except (psycopg2.ProgrammingError, psycopg2.DatabaseError, psycopg2.DatabaseError) as error:

            self.logger.warn(error.message)

            if self.debug:
                warnings.warn('\n' + self.error_status)
            connection.rollback()
        finally:
            self.db.putconn(connection)

    def query(self, query, fetch_opts="many"):
        """Execute postgresql query.
        """
        with self._get_cursor() as cursor:
            cursor.execute(query)
            return getattr(cursor, dict(CURSOR_FETCH).get(fetch_opts))()

    @classmethod
    def _pool_uid_maker(cls, user, host, port, database):
        return cls.pool_uid.format(user, host, port, database)

    @classmethod
    def _pool_factory(cls, pool_type):
        if pool_type not in dict(POOL_TYPE):
            raise EngineError("Pool type invalid string.")
        return getattr(psycopg2.pool, dict(POOL_TYPE).get(pool_type))
