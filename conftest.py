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


class PipelineSpy(object):
    def __init__(self):
        self.method_mocks = {}

    def add_mock(self, method, mock):
        self.method_mocks[method] = mock

    def assert_called(self, method, *args, **kwargs):
        mock = self.method_mocks[method]
        call = mock.call_args

        assert call
        # import ipdb; ipdb.set_trace()

        self._compare_call_args(call, args, kwargs)

    def _compare_call_args(self, call, expected_args, expected_kwargs):
        called_args = call[0][1:]     # ignore first arg, self
        called_kwargs = call[1]

        print 'compare', called_args, called_kwargs

        if called_args:
            assert called_args == expected_args
        if called_kwargs:
            assert called_kwargs == expected_kwargs

    def reset_mock(self):
        for mock in self.method_mocks.values():
            mock.reset_mock()


@pytest.fixture
def pipe(commonsetup, mocker):
    from redis.client import BasePipeline, StrictRedis

    pipe_spy = PipelineSpy()

    for method in redis_methods:
        mock = mocker.spy(StrictRedis, method)
        pipe_spy.add_mock(method, mock)

    for method in pipeline_methods:
        mock = mocker.spy(BasePipeline, method)
        pipe_spy.add_mock(method, mock)

    # return StrictPipeline
    # return PipelineSpy()
    return pipe_spy
