"""Integration between SQLAlchemy and Trino.

Some code based on
https://github.com/zzzeek/sqlalchemy/blob/rel_0_5/lib/sqlalchemy/databases/sqlite.py
which is released under the MIT license.
"""

from __future__ import absolute_import
from __future__ import unicode_literals

import re
from sqlalchemy import exc
from sqlalchemy import types
from sqlalchemy import util
# TODO shouldn't use mysql type
try:
    from sqlalchemy.databases import mysql
    mysql_tinyinteger = mysql.MSTinyInteger
except ImportError:
    # Required for SQLAlchemy>=2.0
    from sqlalchemy.dialects import mysql
    mysql_tinyinteger = mysql.base.MSTinyInteger
from sqlalchemy.engine import default
from sqlalchemy.sql import compiler
from sqlalchemy.sql.compiler import SQLCompiler

from pyhive import trino
from pyhive.common import UniversalSet
from pyhive.sqlalchemy_presto import PrestoDialect, PrestoCompiler, PrestoIdentifierPreparer

class TrinoIdentifierPreparer(PrestoIdentifierPreparer):
    pass


_type_map = {
    'boolean': types.Boolean,
    'tinyint': mysql_tinyinteger,
    'smallint': types.SmallInteger,
    'integer': types.Integer,
    'bigint': types.BigInteger,
    'real': types.Float,
    'double': types.Float,
    'varchar': types.String,
    'timestamp': types.TIMESTAMP,
    'date': types.DATE,
    'varbinary': types.VARBINARY,
}


class TrinoCompiler(PrestoCompiler):
    pass


class TrinoTypeCompiler(PrestoCompiler):
    def visit_CLOB(self, type_, **kw):
        raise ValueError("Trino does not support the CLOB column type.")

    def visit_NCLOB(self, type_, **kw):
        raise ValueError("Trino does not support the NCLOB column type.")

    def visit_DATETIME(self, type_, **kw):
        raise ValueError("Trino does not support the DATETIME column type.")

    def visit_FLOAT(self, type_, **kw):
        return 'DOUBLE'

    def visit_TEXT(self, type_, **kw):
        if type_.length:
            return 'VARCHAR({:d})'.format(type_.length)
        else:
            return 'VARCHAR'


class TrinoDialect(PrestoDialect):
    name = 'trino'
    supports_statement_cache = False

    @classmethod
    def dbapi(cls):
        return trino
    
    @classmethod
    def import_dbapi(cls):
        return trino    
