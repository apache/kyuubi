# encoding: utf-8
from __future__ import absolute_import
from __future__ import unicode_literals
from pyhive import common
import datetime
import unittest


class TestCommon(unittest.TestCase):
    def test_escape_args(self):
        escaper = common.ParamEscaper()
        self.assertEqual(escaper.escape_args({'foo': 'bar'}),
                         {'foo': "'bar'"})
        self.assertEqual(escaper.escape_args({'foo': 123}),
                         {'foo': 123})
        self.assertEqual(escaper.escape_args({'foo': 123.456}),
                         {'foo': 123.456})
        self.assertEqual(escaper.escape_args({'foo': ['a', 'b', 'c']}),
                         {'foo': "('a','b','c')"})
        self.assertEqual(escaper.escape_args({'foo': ('a', 'b', 'c')}),
                         {'foo': "('a','b','c')"})
        self.assertIn(escaper.escape_args({'foo': {'a', 'b'}}),
                      ({'foo': "('a','b')"}, {'foo': "('b','a')"}))
        self.assertIn(escaper.escape_args({'foo': frozenset(['a', 'b'])}),
                      ({'foo': "('a','b')"}, {'foo': "('b','a')"}))

        self.assertEqual(escaper.escape_args(('bar',)),
                         ("'bar'",))
        self.assertEqual(escaper.escape_args([123]),
                         (123,))
        self.assertEqual(escaper.escape_args((123.456,)),
                         (123.456,))
        self.assertEqual(escaper.escape_args((['a', 'b', 'c'],)),
                         ("('a','b','c')",))
        self.assertEqual(escaper.escape_args((['你好', 'b', 'c'],)),
                         ("('你好','b','c')",))
        self.assertEqual(escaper.escape_args((datetime.date(2020, 4, 17),)),
                         ("'2020-04-17'",))
        self.assertEqual(escaper.escape_args((datetime.datetime(2020, 4, 17, 12, 0, 0, 123456),)),
                         ("'2020-04-17 12:00:00.123456'",))
