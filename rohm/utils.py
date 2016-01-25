from contextlib import contextmanager
from datetime import datetime

from pytz import utc


def utcnow():
    return utc.localize(datetime.now())


@contextmanager
def redis_operation(conn, pipelined=False):
    client_conn = conn
    if pipelined:
        client_conn = conn.pipeline()

    yield client_conn

    if pipelined:
        client_conn.execute()


def safe_unicode(value, encoding='utf-8'):

    if isinstance(value, unicode):
        return value
    elif isinstance(value, basestring):
        try:
            value = unicode(value, encoding)
        except UnicodeDecodeError:
            value = value.decode('utf-8', 'replace')

    return value


def safe_string(value, encoding='utf-8'):

    if isinstance(value, basestring):
        return value
    elif isinstance(value, unicode):
        try:
            value = value.encode(encoding)
        except UnicodeEncodeError:
            value = value.encode('utf-8', 'replace')

    return value


def hmget_result_is_nonexistent(result):
    """
    HMGET returns an array of [None, None...] for a non-existent key.
    This helper detects that
    """
    return all(val is None for val in result)
