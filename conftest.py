import pytest
from rohm.connection import get_connection
from rohm import model_registry


@pytest.yield_fixture(autouse=True)
def commonsetup():
    conn = get_connection()
    conn.flushdb()

    model_registry.clear()

    yield None


redis_methods = [
    'get',
    'set',
    'delete',
    'hmset',
    'hdel',
    'expire',
]


@pytest.fixture
def conn(commonsetup, mocker):
    from rohm.models import conn
    from redis.client import StrictPipeline, Pipeline
    mocked = mocker.patch('rohm.models.conn', wraps=conn)

    for method in redis_methods:
        mocker.spy(StrictPipeline, method)
        mocker.spy(Pipeline, method)

    return mocked
