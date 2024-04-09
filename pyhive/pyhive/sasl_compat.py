# Original source of this file is https://github.com/cloudera/impyla/blob/master/impala/sasl_compat.py 
# which uses Apache-2.0 license as of 21 May 2023.
# This code was added to Impyla in 2016 as a compatibility layer to allow use of either python-sasl or pure-sasl 
# via PR https://github.com/cloudera/impyla/pull/179
# Even though thrift_sasl lists pure-sasl as dependency here https://github.com/cloudera/thrift_sasl/blob/master/setup.py#L34 
# but it still calls functions native to python-sasl in this file https://github.com/cloudera/thrift_sasl/blob/master/thrift_sasl/__init__.py#L82
# Hence this code is required for the fallback to work.
 

from puresasl.client import SASLClient, SASLError
from contextlib import contextmanager

@contextmanager
def error_catcher(self, Exc = Exception):
    try:
        self.error = None
        yield
    except Exc as e:
        self.error = str(e)


class PureSASLClient(SASLClient):
    def __init__(self, *args, **kwargs):
        self.error = None
        super(PureSASLClient, self).__init__(*args, **kwargs)

    def start(self, mechanism):
        with error_catcher(self, SASLError):
            if isinstance(mechanism, list):
                self.choose_mechanism(mechanism)
            else:
                self.choose_mechanism([mechanism])
            return True, self.mechanism, self.process()
        # else
        return False, mechanism, None

    def encode(self, incoming):
        with error_catcher(self):
            return True, self.unwrap(incoming)
        # else
        return False, None

    def decode(self, outgoing):
        with error_catcher(self):
            return True, self.wrap(outgoing)
        # else
        return False, None

    def step(self, challenge=None):
        with error_catcher(self):
            return True, self.process(challenge)
        # else
        return False, None

    def getError(self):
        return self.error
