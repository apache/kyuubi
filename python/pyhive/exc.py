"""
Package private common utilities. Do not use directly.
"""
from __future__ import absolute_import
from __future__ import unicode_literals

__all__ = [
    'Error', 'Warning', 'InterfaceError', 'DatabaseError', 'InternalError', 'OperationalError',
    'ProgrammingError', 'DataError', 'NotSupportedError',
]


class Error(Exception):
    """Exception that is the base class of all other error exceptions.

    You can use this to catch all errors with one single except statement.
    """
    pass


class Warning(Exception):
    """Exception raised for important warnings like data truncations while inserting, etc."""
    pass


class InterfaceError(Error):
    """Exception raised for errors that are related to the database interface rather than the
    database itself.
    """
    pass


class DatabaseError(Error):
    """Exception raised for errors that are related to the database."""
    pass


class InternalError(DatabaseError):
    """Exception raised when the database encounters an internal error, e.g. the cursor is not valid
    anymore, the transaction is out of sync, etc."""
    pass


class OperationalError(DatabaseError):
    """Exception raised for errors that are related to the database's operation and not necessarily
    under the control of the programmer, e.g. an unexpected disconnect occurs, the data source name
    is not found, a transaction could not be processed, a memory allocation error occurred during
    processing, etc.
    """
    pass


class ProgrammingError(DatabaseError):
    """Exception raised for programming errors, e.g. table not found or already exists, syntax error
    in the SQL statement, wrong number of parameters specified, etc.
    """
    pass


class DataError(DatabaseError):
    """Exception raised for errors that are due to problems with the processed data like division by
    zero, numeric value out of range, etc.
    """
    pass


class NotSupportedError(DatabaseError):
    """Exception raised in case a method or database API was used which is not supported by the
    database, e.g. requesting a ``.rollback()`` on a connection that does not support transaction or
    has transactions turned off.
    """
    pass
