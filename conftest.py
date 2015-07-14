import pytest
from rohm.connection import get_connection
from rohm import model_registry


@pytest.yield_fixture(autouse=True)
def commonsetup():
    conn = get_connection()
    conn.flushdb()

    model_registry.clear()

    yield None


@pytest.fixture
def mockconn(commonsetup, mocker):
    from rohm.models import conn
    mocked = mocker.patch('rohm.models.conn', wraps=conn)
    return mocked
    # import rohm.models

    # mocked = mocker.spy()
    # mocker.spy(rohm.models.conn, 'get')
    # mocker.spy(rohm.models.conn, 'set')
    # mocker.spy(rohm.models.conn, 'delete')
    # mocker.spy(rohm.models.conn, 'hgetall')

    # import pdb; pdb.set_trace()

    # return rohm.models.conn
