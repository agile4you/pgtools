# -*- coding: utf-8 -*-
"""`pgtools.pubsub` module.

Provides a nice API over Postgresql LISTEN / NOTIFY.
"""

from __future__ import absolute_import


import select
import logging

import psycopg2

logger = logging.getLogger(__name__)

NOT_READY = ([], [], [])


class PubSub(object):
    def __init__(self, conn):
        assert conn.autocommit, "Connection must be in autocommit mode."
        self.conn = conn

    def listen(self, channel):
        with self.conn.cursor() as cur:
            cur.execute('LISTEN %s;' % channel)

    def unlisten(self, channel):
        with self.conn.cursor() as cur:
            cur.execute('UNLISTEN %s;' % channel)

    def notify(self, channel, payload):
        with self.conn.cursor() as cur:
            cur.execute('SELECT pg_notify(%s, %s);', (channel, payload))

    def get_event(self, select_timeout=0):
        # poll the connection, then return one event, if we have one.  Else
        # return None.
        select.select([self.conn], [], [], select_timeout)
        self.conn.poll()
        if self.conn.notifies:
            return self.conn.notifies.pop(0)

    def get_events(self, select_timeout=0):
        # Poll the connection and return all events, if there are any.  Else
        # return None.
        select.select([self.conn], [], [], select_timeout)  # redundant?
        self.conn.poll()
        events = []
        while self.conn.notifies:
            events.append(self.conn.notifies.pop(0))
        if events:
            return events

    def events(self, select_timeout=5, yield_timeouts=False):
        while True:
            if select.select([self.conn], [], [], select_timeout) == NOT_READY:
                if yield_timeouts:
                    yield None
            else:
                self.conn.poll()
                while self.conn.notifies:
                    yield self.conn.notifies.pop(0)

    def close(self):
        self.conn.close()


def connect(*args, **kwargs):
    conn = psycopg2.connect(*args, **kwargs)
    conn.autocommit = True
    return PubSub(conn)
