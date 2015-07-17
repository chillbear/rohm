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
    'hmget',
    'hset',
    'hget',
    'hgetall',
    'hdel',
    'expire',
    'pipeline',
]

pipeline_methods = [
    'execute'
]


@pytest.fixture
def conn(commonsetup, mocker):
    from rohm.models import conn

    mocked = mocker.patch('rohm.models.conn', wraps=conn)

    return mocked


@pytest.fixture
def pipe(commonsetup, mocker):
    from redis.client import BasePipeline, StrictRedis, StrictPipeline

    for method in redis_methods:
        mocker.spy(StrictRedis, method)

    for method in pipeline_methods:
        mocker.spy(BasePipeline, method)

    return StrictPipeline
