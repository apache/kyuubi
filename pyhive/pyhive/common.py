"""Package private common utilities. Do not use directly.

Many docstrings in this file are based on PEP-249, which is in the public domain.
"""

from __future__ import absolute_import
from __future__ import unicode_literals
from builtins import bytes
from builtins import int
from builtins import object
from builtins import str
from past.builtins import basestring
from pyhive import exc
import abc
import collections
import time
import datetime
from future.utils import with_metaclass
from itertools import islice

try:
    from collections.abc import Iterable
except ImportError:
    from collections import Iterable


class DBAPICursor(with_metaclass(abc.ABCMeta, object)):
    """Base class for some common DB-API logic"""

    _STATE_NONE = 0
    _STATE_RUNNING = 1
    _STATE_FINISHED = 2

    def __init__(self, poll_interval=1):
        self._poll_interval = poll_interval
        self._reset_state()
        self.lastrowid = None

    def _reset_state(self):
        """Reset state about the previous query in preparation for running another query"""
        # State to return as part of DB-API
        self._rownumber = 0

        # Internal helper state
        self._state = self._STATE_NONE
        self._data = collections.deque()
        self._columns = None

    def _fetch_while(self, fn):
        while fn():
            self._fetch_more()
            if fn():
                time.sleep(self._poll_interval)

    @abc.abstractproperty
    def description(self):
        raise NotImplementedError  # pragma: no cover

    def close(self):
        """By default, do nothing"""
        pass

    @abc.abstractmethod
    def _fetch_more(self):
        """Get more results, append it to ``self._data``, and update ``self._state``."""
        raise NotImplementedError  # pragma: no cover

    @property
    def rowcount(self):
        """By default, return -1 to indicate that this is not supported."""
        return -1

    @abc.abstractmethod
    def execute(self, operation, parameters=None):
        """Prepare and execute a database operation (query or command).

        Parameters may be provided as sequence or mapping and will be bound to variables in the
        operation. Variables are specified in a database-specific notation (see the module's
        ``paramstyle`` attribute for details).

        Return values are not defined.
        """
        raise NotImplementedError  # pragma: no cover

    def executemany(self, operation, seq_of_parameters):
        """Prepare a database operation (query or command) and then execute it against all parameter
        sequences or mappings found in the sequence ``seq_of_parameters``.

        Only the final result set is retained.

        Return values are not defined.
        """
        for parameters in seq_of_parameters[:-1]:
            self.execute(operation, parameters)
            while self._state != self._STATE_FINISHED:
                self._fetch_more()
        if seq_of_parameters:
            self.execute(operation, seq_of_parameters[-1])

    def fetchone(self):
        """Fetch the next row of a query result set, returning a single sequence, or ``None`` when
        no more data is available.

        An :py:class:`~pyhive.exc.Error` (or subclass) exception is raised if the previous call to
        :py:meth:`execute` did not produce any result set or no call was issued yet.
        """
        if self._state == self._STATE_NONE:
            raise exc.ProgrammingError("No query yet")

        # Sleep until we're done or we have some data to return
        self._fetch_while(lambda: not self._data and self._state != self._STATE_FINISHED)

        if not self._data:
            return None
        else:
            self._rownumber += 1
            return self._data.popleft()

    def fetchmany(self, size=None):
        """Fetch the next set of rows of a query result, returning a sequence of sequences (e.g. a
        list of tuples). An empty sequence is returned when no more rows are available.

        The number of rows to fetch per call is specified by the parameter. If it is not given, the
        cursor's arraysize determines the number of rows to be fetched. The method should try to
        fetch as many rows as indicated by the size parameter. If this is not possible due to the
        specified number of rows not being available, fewer rows may be returned.

        An :py:class:`~pyhive.exc.Error` (or subclass) exception is raised if the previous call to
        :py:meth:`execute` did not produce any result set or no call was issued yet.
        """
        if size is None:
            size = self.arraysize
        return list(islice(iter(self.fetchone, None), size))

    def fetchall(self):
        """Fetch all (remaining) rows of a query result, returning them as a sequence of sequences
        (e.g. a list of tuples).

        An :py:class:`~pyhive.exc.Error` (or subclass) exception is raised if the previous call to
        :py:meth:`execute` did not produce any result set or no call was issued yet.
        """
        return list(iter(self.fetchone, None))

    @property
    def arraysize(self):
        """This read/write attribute specifies the number of rows to fetch at a time with
        :py:meth:`fetchmany`. It defaults to 1 meaning to fetch a single row at a time.
        """
        return self._arraysize

    @arraysize.setter
    def arraysize(self, value):
        self._arraysize = value

    def setinputsizes(self, sizes):
        """Does nothing by default"""
        pass

    def setoutputsize(self, size, column=None):
        """Does nothing by default"""
        pass

    #
    # Optional DB API Extensions
    #

    @property
    def rownumber(self):
        """This read-only attribute should provide the current 0-based index of the cursor in the
        result set.

        The index can be seen as index of the cursor in a sequence (the result set). The next fetch
        operation will fetch the row indexed by ``rownumber`` in that sequence.
        """
        return self._rownumber

    def __next__(self):
        """Return the next row from the currently executing SQL statement using the same semantics
        as :py:meth:`fetchone`. A ``StopIteration`` exception is raised when the result set is
        exhausted.
        """
        one = self.fetchone()
        if one is None:
            raise StopIteration
        else:
            return one

    next = __next__

    def __iter__(self):
        """Return self to make cursors compatible to the iteration protocol."""
        return self


class DBAPITypeObject(object):
    # Taken from http://www.python.org/dev/peps/pep-0249/#implementation-hints
    def __init__(self, *values):
        self.values = values

    def __cmp__(self, other):
        if other in self.values:
            return 0
        if other < self.values:
            return 1
        else:
            return -1


class ParamEscaper(object):
    _DATE_FORMAT = "%Y-%m-%d"
    _TIME_FORMAT = "%H:%M:%S.%f"
    _DATETIME_FORMAT = "{} {}".format(_DATE_FORMAT, _TIME_FORMAT)

    def escape_args(self, parameters):
        if isinstance(parameters, dict):
            return {k: self.escape_item(v) for k, v in parameters.items()}
        elif isinstance(parameters, (list, tuple)):
            return tuple(self.escape_item(x) for x in parameters)
        else:
            raise exc.ProgrammingError("Unsupported param format: {}".format(parameters))

    def escape_number(self, item):
        return item

    def escape_string(self, item):
        # Need to decode UTF-8 because of old sqlalchemy.
        # Newer SQLAlchemy checks dialect.supports_unicode_binds before encoding Unicode strings
        # as byte strings. The old version always encodes Unicode as byte strings, which breaks
        # string formatting here.
        if isinstance(item, bytes):
            item = item.decode('utf-8')
        # This is good enough when backslashes are literal, newlines are just followed, and the way
        # to escape a single quote is to put two single quotes.
        # (i.e. only special character is single quote)
        return "'{}'".format(item.replace("'", "''"))

    def escape_sequence(self, item):
        l = map(str, map(self.escape_item, item))
        return '(' + ','.join(l) + ')'

    def escape_datetime(self, item, format, cutoff=0):
        dt_str = item.strftime(format)
        formatted = dt_str[:-cutoff] if cutoff and format.endswith(".%f") else dt_str
        return "'{}'".format(formatted)

    def escape_item(self, item):
        if item is None:
            return 'NULL'
        elif isinstance(item, (int, float)):
            return self.escape_number(item)
        elif isinstance(item, basestring):
            return self.escape_string(item)
        elif isinstance(item, Iterable):
            return self.escape_sequence(item)
        elif isinstance(item, datetime.datetime):
            return self.escape_datetime(item, self._DATETIME_FORMAT)
        elif isinstance(item, datetime.date):
            return self.escape_datetime(item, self._DATE_FORMAT)
        else:
            raise exc.ProgrammingError("Unsupported object {}".format(item))


class UniversalSet(object):
    """set containing everything"""
    def __contains__(self, item):
        return True
