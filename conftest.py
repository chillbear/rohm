import pytest
from rohm.connection import get_connection
from rohm import model_registry
import mock


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

        spy = MethodSpy(mock)
        setattr(self, method, spy)   # make dot accessible

    def assert_called_with(self, method, *args, **kwargs):
        mock = getattr(self, method)
        mock.assert_called_with(*args, **kwargs)

    def reset_mocks(self):
        for mock in self.method_mocks.values():
            mock.reset_mock()


class MethodSpy(object):
    def __init__(self, mock):
        # this is the mocked method
        self.mock = mock

    def assert_called_with(self, *args, **kwargs):
        call = self.mock.call_args

        self._compare_call_args(call, args, kwargs)

    def _convert_call(self, call):
        args = call[0][1:]   # erase first arg
        kwargs = call[1]
        return mock.call(*args, **kwargs)

    @property
    def call_args(self):
        _call_args = self.mock.call_args
        return self._convert_call(_call_args)

    @property
    def call_args_list(self):
        _call_args_list = self.mock.call_args_list
        return [self._convert_call(call) for call in _call_args_list]

    def _compare_call_args(self, call, expected_args, expected_kwargs):
        called_args = call[0][1:]     # ignore first arg, self
        called_kwargs = call[1]

        if called_args:
            assert called_args == expected_args
        if called_kwargs:
            assert called_kwargs == expected_kwargs

    def __getattr__(self, name):
        """
        Forward all attribute calls..?
        """
        return getattr(self.mock, name)


@pytest.fixture
def pipe(commonsetup, mocker):
    from redis.client import BasePipeline, StrictRedis, StrictPipeline

    pipe_spy = PipelineSpy()
    # because of inheritance, we just pass the actual method we care about

    for method in redis_methods:
        mocker.spy(StrictRedis, method)
        pipe_spy.add_mock(method, getattr(StrictPipeline, method))

    for method in pipeline_methods:
        mocker.spy(BasePipeline, method)
        pipe_spy.add_mock(method, getattr(StrictPipeline, method))

    # return StrictPipeline
    # return PipelineSpy()
    return pipe_spy
