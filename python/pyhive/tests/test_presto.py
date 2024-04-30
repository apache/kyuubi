"""Presto integration tests.

These rely on having a Presto+Hadoop cluster set up.
They also require a tables created by make_test_tables.sh.
"""

from __future__ import absolute_import
from __future__ import unicode_literals

import contextlib
import os
from decimal import Decimal

import requests

import pytest
from pyhive import exc
from pyhive import presto
from pyhive.tests.dbapi_test_case import DBAPITestCase
from pyhive.tests.dbapi_test_case import with_cursor
import mock
import unittest
import datetime

_HOST = 'localhost'
_PORT = '8080'


class TestPresto(unittest.TestCase, DBAPITestCase):
    __test__ = True

    def connect(self):
        return presto.connect(host=_HOST, port=_PORT, source=self.id())

    def test_bad_protocol(self):
        self.assertRaisesRegexp(ValueError, 'Protocol must be',
                                lambda: presto.connect('localhost', protocol='nonsense').cursor())

    def test_escape_args(self):
        escaper = presto.PrestoParamEscaper()

        self.assertEqual(escaper.escape_args((datetime.date(2020, 4, 17),)),
                         ("date '2020-04-17'",))
        self.assertEqual(escaper.escape_args((datetime.datetime(2020, 4, 17, 12, 0, 0, 123456),)),
                         ("timestamp '2020-04-17 12:00:00.123'",))

    @with_cursor
    def test_description(self, cursor):
        cursor.execute('SELECT 1 AS foobar FROM one_row')
        self.assertEqual(cursor.description, [('foobar', 'integer', None, None, None, None, True)])
        self.assertIsNotNone(cursor.last_query_id)

    @with_cursor
    def test_complex(self, cursor):
        cursor.execute('SELECT * FROM one_row_complex')
        # TODO Presto drops the union field
        if os.environ.get('PRESTO') == '0.147':
            tinyint_type = 'integer'
            smallint_type = 'integer'
            float_type = 'double'
        else:
            # some later version made these map to more specific types
            tinyint_type = 'tinyint'
            smallint_type = 'smallint'
            float_type = 'real'
        self.assertEqual(cursor.description, [
            ('boolean', 'boolean', None, None, None, None, True),
            ('tinyint', tinyint_type, None, None, None, None, True),
            ('smallint', smallint_type, None, None, None, None, True),
            ('int', 'integer', None, None, None, None, True),
            ('bigint', 'bigint', None, None, None, None, True),
            ('float', float_type, None, None, None, None, True),
            ('double', 'double', None, None, None, None, True),
            ('string', 'varchar', None, None, None, None, True),
            ('timestamp', 'timestamp', None, None, None, None, True),
            ('binary', 'varbinary', None, None, None, None, True),
            ('array', 'array(integer)', None, None, None, None, True),
            ('map', 'map(integer,integer)', None, None, None, None, True),
            ('struct', 'row(a integer,b integer)', None, None, None, None, True),
            # ('union', 'varchar', None, None, None, None, True),
            ('decimal', 'decimal(10,1)', None, None, None, None, True),
        ])
        rows = cursor.fetchall()
        expected = [(
            True,
            127,
            32767,
            2147483647,
            9223372036854775807,
            0.5,
            0.25,
            'a string',
            '1970-01-01 00:00:00.000',
            b'123',
            [1, 2],
            {"1": 2, "3": 4},  # Presto converts all keys to strings so that they're valid JSON
            [1, 2],  # struct is returned as a list of elements
            # '{0:1}',
            Decimal('0.1'),
        )]
        self.assertEqual(rows, expected)
        # catch unicode/str
        self.assertEqual(list(map(type, rows[0])), list(map(type, expected[0])))

    @with_cursor
    def test_cancel(self, cursor):
        cursor.execute(
            "SELECT a.a * rand(), b.a * rand()"
            "FROM many_rows a "
            "CROSS JOIN many_rows b "
        )
        self.assertIn(cursor.poll()['stats']['state'], (
            'STARTING', 'PLANNING', 'RUNNING', 'WAITING_FOR_RESOURCES', 'QUEUED'))
        cursor.cancel()
        self.assertIsNotNone(cursor.last_query_id)
        self.assertIsNone(cursor.poll())

    def test_noops(self):
        """The DB-API specification requires that certain actions exist, even though they might not
        be applicable."""
        # Wohoo inflating coverage stats!
        connection = self.connect()
        cursor = connection.cursor()
        self.assertEqual(cursor.rowcount, -1)
        cursor.setinputsizes([])
        cursor.setoutputsize(1, 'blah')
        self.assertIsNone(cursor.last_query_id)
        connection.commit()

    @mock.patch('requests.post')
    def test_non_200(self, post):
        cursor = self.connect().cursor()
        post.return_value.status_code = 404
        self.assertRaises(exc.OperationalError, lambda: cursor.execute('show tables'))

    @with_cursor
    def test_poll(self, cursor):
        self.assertRaises(presto.ProgrammingError, cursor.poll)

        cursor.execute('SELECT * FROM one_row')
        while True:
            status = cursor.poll()
            if status is None:
                break
            self.assertIn('stats', status)

        def fail(*args, **kwargs):
            self.fail("Should not need requests.get after done polling")  # pragma: no cover

        with mock.patch('requests.get', fail):
            self.assertEqual(cursor.fetchall(), [(1,)])

    @with_cursor
    def test_set_session(self, cursor):
        id = None
        self.assertIsNone(cursor.last_query_id)
        cursor.execute("SET SESSION query_max_run_time = '1234m'")
        self.assertIsNotNone(cursor.last_query_id)
        id = cursor.last_query_id
        cursor.fetchall()
        self.assertEqual(id, cursor.last_query_id)

        cursor.execute('SHOW SESSION')
        self.assertIsNotNone(cursor.last_query_id)
        self.assertNotEqual(id, cursor.last_query_id)
        id = cursor.last_query_id
        rows = [r for r in cursor.fetchall() if r[0] == 'query_max_run_time']
        self.assertEqual(len(rows), 1)
        session_prop = rows[0]
        self.assertEqual(session_prop[1], '1234m')
        self.assertEqual(id, cursor.last_query_id)

        cursor.execute('RESET SESSION query_max_run_time')
        self.assertIsNotNone(cursor.last_query_id)
        self.assertNotEqual(id, cursor.last_query_id)
        id = cursor.last_query_id
        cursor.fetchall()
        self.assertEqual(id, cursor.last_query_id)

        cursor.execute('SHOW SESSION')
        self.assertIsNotNone(cursor.last_query_id)
        self.assertNotEqual(id, cursor.last_query_id)
        id = cursor.last_query_id
        rows = [r for r in cursor.fetchall() if r[0] == 'query_max_run_time']
        self.assertEqual(len(rows), 1)
        session_prop = rows[0]
        self.assertNotEqual(session_prop[1], '1234m')
        self.assertEqual(id, cursor.last_query_id)

    def test_set_session_in_constructor(self):
        conn = presto.connect(
            host=_HOST, source=self.id(), session_props={'query_max_run_time': '1234m'}
        )
        with contextlib.closing(conn):
            with contextlib.closing(conn.cursor()) as cursor:
                cursor.execute('SHOW SESSION')
                rows = [r for r in cursor.fetchall() if r[0] == 'query_max_run_time']
                assert len(rows) == 1
                session_prop = rows[0]
                assert session_prop[1] == '1234m'

                cursor.execute('RESET SESSION query_max_run_time')
                cursor.fetchall()

                cursor.execute('SHOW SESSION')
                rows = [r for r in cursor.fetchall() if r[0] == 'query_max_run_time']
                assert len(rows) == 1
                session_prop = rows[0]
                assert session_prop[1] != '1234m'

    def test_invalid_protocol_config(self):
        """protocol should be https when passing password"""
        self.assertRaisesRegexp(
            ValueError, 'Protocol.*https.*password', lambda: presto.connect(
                host=_HOST, username='user', password='secret', protocol='http').cursor()
        )

    def test_invalid_password_and_kwargs(self):
        """password and requests_kwargs authentication are incompatible"""
        self.assertRaisesRegexp(
            ValueError, 'Cannot use both', lambda: presto.connect(
                host=_HOST, username='user', password='secret', protocol='https',
                requests_kwargs={'auth': requests.auth.HTTPBasicAuth('user', 'secret')}
            ).cursor()
        )

    def test_invalid_kwargs(self):
        """some kwargs are reserved"""
        self.assertRaisesRegexp(
            ValueError, 'Cannot override', lambda: presto.connect(
                host=_HOST, username='user', requests_kwargs={'url': 'test'}
            ).cursor()
        )

    @pytest.mark.skip(reason='This test requires a proxy server running on localhost:9999')
    def test_requests_kwargs(self):
        connection = presto.connect(
            host=_HOST, port=_PORT, source=self.id(),
            requests_kwargs={'proxies': {'http': 'localhost:9999'}},
        )
        cursor = connection.cursor()
        self.assertRaises(requests.exceptions.ProxyError,
                          lambda: cursor.execute('SELECT * FROM one_row'))

    def test_requests_session(self):
        with requests.Session() as session:
            connection = presto.connect(
                host=_HOST, port=_PORT, source=self.id(), requests_session=session
            )
            cursor = connection.cursor()
            cursor.execute('SELECT * FROM one_row')
            self.assertEqual(cursor.fetchall(), [(1,)])
