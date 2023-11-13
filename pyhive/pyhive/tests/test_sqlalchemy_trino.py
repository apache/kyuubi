from sqlalchemy.engine import create_engine
from pyhive.tests.sqlalchemy_test_case import SqlAlchemyTestCase
from pyhive.tests.sqlalchemy_test_case import with_engine_connection
from sqlalchemy.exc import NoSuchTableError, DatabaseError
from sqlalchemy.schema import MetaData, Table, Column
from sqlalchemy.types import String
from sqlalchemy.sql import text
from sqlalchemy import types
from decimal import Decimal

import unittest
import contextlib


class TestSqlAlchemyTrino(unittest.TestCase, SqlAlchemyTestCase):
    def create_engine(self):
        return create_engine('trino+pyhive://localhost:18080/hive/default?source={}'.format(self.id()))
    
    def test_bad_format(self):
        self.assertRaises(
            ValueError,
            lambda: create_engine('trino+pyhive://localhost:18080/hive/default/what'),
        )

    @with_engine_connection
    def test_reflect_select(self, engine, connection):
        """reflecttable should be able to fill in a table from the name"""
        one_row_complex = Table('one_row_complex', MetaData(), autoload_with=engine)
        # Presto ignores the union column
        self.assertEqual(len(one_row_complex.c), 15 - 1)
        self.assertIsInstance(one_row_complex.c.string, Column)
        rows = connection.execute(one_row_complex.select()).fetchall()
        self.assertEqual(len(rows), 1)
        self.assertEqual(list(rows[0]), [
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
            {"1": 2, "3": 4},
            [1, 2],
            Decimal('0.1'),
        ])

        self.assertIsInstance(one_row_complex.c.boolean.type, types.Boolean)
        self.assertIsInstance(one_row_complex.c.tinyint.type, types.Integer)
        self.assertIsInstance(one_row_complex.c.smallint.type, types.Integer)
        self.assertIsInstance(one_row_complex.c.int.type, types.Integer)
        self.assertIsInstance(one_row_complex.c.bigint.type, types.BigInteger)
        self.assertIsInstance(one_row_complex.c.float.type, types.Float)
        self.assertIsInstance(one_row_complex.c.double.type, types.Float)
        self.assertIsInstance(one_row_complex.c.string.type, String)
        self.assertIsInstance(one_row_complex.c.timestamp.type, types.NullType)
        self.assertIsInstance(one_row_complex.c.binary.type, types.VARBINARY)
        self.assertIsInstance(one_row_complex.c.array.type, types.NullType)
        self.assertIsInstance(one_row_complex.c.map.type, types.NullType)
        self.assertIsInstance(one_row_complex.c.struct.type, types.NullType)
        self.assertIsInstance(one_row_complex.c.decimal.type, types.NullType)
    
    @with_engine_connection
    def test_reflect_no_such_table(self, engine, connection):
        """reflecttable should throw an exception on an invalid table"""
        self.assertRaises(
            NoSuchTableError,
            lambda: Table('this_does_not_exist', MetaData(), autoload_with=engine))
        self.assertRaises(
            DatabaseError,
            lambda: Table('this_does_not_exist', MetaData(schema="also_does_not_exist"), autoload_with=engine))

    def test_url_default(self):
        engine = create_engine('trino+pyhive://localhost:18080/hive')
        try:
            with contextlib.closing(engine.connect()) as connection:
                self.assertEqual(connection.execute(text('SELECT 1 AS foobar FROM one_row')).scalar(), 1)
        finally:
            engine.dispose()

    @with_engine_connection
    def test_reserved_words(self, engine, connection):
        """Trino uses double quotes, not backticks"""
        # Use keywords for the table/column name
        fake_table = Table('select', MetaData(), Column('current_timestamp', String))
        query = str(fake_table.select().where(fake_table.c.current_timestamp == 'a').compile(engine))
        self.assertIn('"select"', query)
        self.assertIn('"current_timestamp"', query)
        self.assertNotIn('`select`', query)
        self.assertNotIn('`current_timestamp`', query)
