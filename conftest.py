import pytest
from rohm.connection import get_default_connection
from rohm import model_registry
import mock
import logging


@pytest.fixture(scope='session', autouse=True)
def initialsetup():
    logging.basicConfig(
        level=logging.DEBUG,
        format="[%(asctime)s] [%(levelname)s] [%(name)s:%(lineno)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )


@pytest.yield_fixture(autouse=True)
def commonsetup():
    conn = get_default_connection()
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
    # For now forgo the mocking stuff, a bit hard to get right
    from rohm.connection import get_connection
    return get_connection()


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

    def reset_mock(self):
        # Reset all method mocks
        for _mock in self.method_mocks.values():
            _mock.reset_mock()


class MethodSpy(object):
    def __init__(self, mock):
        # this is the mocked method
        self.mock = mock

    def assert_called_with(self, *args, **kwargs):
        call = self.call_args
        assert call == mock.call(*args, **kwargs)

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

    return pipe_spy
