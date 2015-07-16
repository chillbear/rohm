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
