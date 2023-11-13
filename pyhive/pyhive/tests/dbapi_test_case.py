# encoding: utf-8
"""Shared DB-API test cases"""

from __future__ import absolute_import
from __future__ import unicode_literals
from builtins import object
from builtins import range
from future.utils import with_metaclass
from pyhive import exc
import abc
import contextlib
import functools


def with_cursor(fn):
    """Pass a cursor to the given function and handle cleanup.

    The cursor is taken from ``self.connect()``.
    """
    @functools.wraps(fn)
    def wrapped_fn(self, *args, **kwargs):
        with contextlib.closing(self.connect()) as connection:
            with contextlib.closing(connection.cursor()) as cursor:
                fn(self, cursor, *args, **kwargs)
    return wrapped_fn


class DBAPITestCase(with_metaclass(abc.ABCMeta, object)):
    @abc.abstractmethod
    def connect(self):
        raise NotImplementedError  # pragma: no cover

    @with_cursor
    def test_fetchone(self, cursor):
        cursor.execute('SELECT * FROM one_row')
        self.assertEqual(cursor.rownumber, 0)
        self.assertEqual(cursor.fetchone(), (1,))
        self.assertEqual(cursor.rownumber, 1)
        self.assertIsNone(cursor.fetchone())

    @with_cursor
    def test_fetchall(self, cursor):
        cursor.execute('SELECT * FROM one_row')
        self.assertEqual(cursor.fetchall(), [(1,)])
        cursor.execute('SELECT a FROM many_rows ORDER BY a')
        self.assertEqual(cursor.fetchall(), [(i,) for i in range(10000)])

    @with_cursor
    def test_null_param(self, cursor):
        cursor.execute('SELECT %s FROM one_row', (None,))
        self.assertEqual(cursor.fetchall(), [(None,)])

    @with_cursor
    def test_iterator(self, cursor):
        cursor.execute('SELECT * FROM one_row')
        self.assertEqual(list(cursor), [(1,)])
        self.assertRaises(StopIteration, cursor.__next__)

    @with_cursor
    def test_description_initial(self, cursor):
        self.assertIsNone(cursor.description)

    @with_cursor
    def test_description_failed(self, cursor):
        try:
            cursor.execute('blah_blah')
            self.assertIsNone(cursor.description)
        except exc.DatabaseError:
            pass

    @with_cursor
    def test_bad_query(self, cursor):
        def run():
            cursor.execute('SELECT does_not_exist FROM this_really_does_not_exist')
            cursor.fetchone()
        self.assertRaises(exc.DatabaseError, run)

    @with_cursor
    def test_concurrent_execution(self, cursor):
        cursor.execute('SELECT * FROM one_row')
        cursor.execute('SELECT * FROM one_row')
        self.assertEqual(cursor.fetchall(), [(1,)])

    @with_cursor
    def test_executemany(self, cursor):
        for length in 1, 2:
            cursor.executemany(
                'SELECT %(x)d FROM one_row',
                [{'x': i} for i in range(1, length + 1)]
            )
            self.assertEqual(cursor.fetchall(), [(length,)])

    @with_cursor
    def test_executemany_none(self, cursor):
        cursor.executemany('should_never_get_used', [])
        self.assertIsNone(cursor.description)
        self.assertRaises(exc.ProgrammingError, cursor.fetchone)

    @with_cursor
    def test_fetchone_no_data(self, cursor):
        self.assertRaises(exc.ProgrammingError, cursor.fetchone)

    @with_cursor
    def test_fetchmany(self, cursor):
        cursor.execute('SELECT * FROM many_rows LIMIT 15')
        self.assertEqual(cursor.fetchmany(0), [])
        self.assertEqual(len(cursor.fetchmany(10)), 10)
        self.assertEqual(len(cursor.fetchmany(10)), 5)

    @with_cursor
    def test_arraysize(self, cursor):
        cursor.arraysize = 5
        cursor.execute('SELECT * FROM many_rows LIMIT 20')
        self.assertEqual(len(cursor.fetchmany()), 5)

    @with_cursor
    def test_polling_loop(self, cursor):
        """Try to trigger the polling logic in fetchone()"""
        cursor._poll_interval = 0
        cursor.execute('SELECT COUNT(*) FROM many_rows')
        self.assertEqual(cursor.fetchone(), (10000,))

    @with_cursor
    def test_no_params(self, cursor):
        cursor.execute("SELECT '%(x)s' FROM one_row")
        self.assertEqual(cursor.fetchall(), [('%(x)s',)])

    def test_escape(self):
        """Verify that funny characters can be escaped as strings and SELECTed back"""
        bad_str = '''`~!@#$%^&*()_+-={}[]|\\;:'",./<>?\n\r\t '''
        self.run_escape_case(bad_str)

    @with_cursor
    def run_escape_case(self, cursor, bad_str):
        cursor.execute(
            'SELECT %d, %s FROM one_row',
            (1, bad_str)
        )
        self.assertEqual(cursor.fetchall(), [(1, bad_str,)])
        cursor.execute(
            'SELECT %(a)d, %(b)s FROM one_row',
            {'a': 1, 'b': bad_str}
        )
        self.assertEqual(cursor.fetchall(), [(1, bad_str)])

    @with_cursor
    def test_invalid_params(self, cursor):
        self.assertRaises(exc.ProgrammingError, lambda: cursor.execute('', 'hi'))
        self.assertRaises(exc.ProgrammingError, lambda: cursor.execute('', [object]))

    def test_open_close(self):
        with contextlib.closing(self.connect()):
            pass
        with contextlib.closing(self.connect()) as connection:
            with contextlib.closing(connection.cursor()):
                pass

    @with_cursor
    def test_unicode(self, cursor):
        unicode_str = "王兢"
        cursor.execute(
            'SELECT %s FROM one_row',
            (unicode_str,)
        )
        self.assertEqual(cursor.fetchall(), [(unicode_str,)])

    @with_cursor
    def test_null(self, cursor):
        cursor.execute('SELECT null FROM many_rows')
        self.assertEqual(cursor.fetchall(), [(None,)] * 10000)
        cursor.execute('SELECT IF(a % 11 = 0, null, a) FROM many_rows')
        self.assertEqual(cursor.fetchall(), [(None if a % 11 == 0 else a,) for a in range(10000)])

    @with_cursor
    def test_sql_where_in(self, cursor):
        cursor.execute('SELECT * FROM many_rows where a in %s', ([1, 2, 3],))
        self.assertEqual(len(cursor.fetchall()), 3)
        cursor.execute('SELECT * FROM many_rows where b in %s limit 10',
                       (['blah'],))
        self.assertEqual(len(cursor.fetchall()), 10)
