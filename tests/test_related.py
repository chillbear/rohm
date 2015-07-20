import pytest
from mock import call

from rohm.models import Model
from rohm import fields


@pytest.fixture
def Foo():
    class Foo(Model):
        name = fields.CharField()
        bar = fields.RelatedModelField('Bar')
    return Foo


@pytest.fixture
def Bar():
    class Bar(Model):
        title = fields.CharField()
    return Bar


def test_related(Foo, Bar, conn, pipe):

    key = 'foo:1'

    bar1 = Bar(id=1, title='bar1')
    bar1.save()

    bar2 = Bar(id=2, title='bar2')
    bar2.save()

    foo = Foo(id=1, name='foo', bar=bar1)
    foo.save()

    foo = Foo.get(1)

    assert pipe.hgetall.call_count == 1
    assert pipe.hgetall.call_args == call(key)

    conn.reset_mock()

    # Check internal stuff
    assert set(foo._data.keys()) == {'id', 'name', 'bar_id'}
    assert foo._loaded_related_field_data == {}
    assert foo._loaded_field_names == {'id', 'name', 'bar_id'}

    # Reading bar_id should not access Redis
    assert foo.bar_id == 1
    assert conn.mock_calls == []

    # Should only read related field here
    pipe.reset_mocks()

    bar = foo.bar
    assert bar.title == 'bar1'
    # assert conn.hgetall.call_args_list == [call('bar:1')]
    assert pipe.hgetall.call_args_list == [call('bar:1')]

    # _data should not contain related Bar
    assert set(foo._data.keys()) == {'id', 'name', 'bar_id'}
    assert foo._loaded_related_field_data == {'bar': bar}
    assert foo._loaded_field_names == {'id', 'name', 'bar_id'}

    # Reassign bar
    conn.reset_mock()
    foo.bar = bar2
    foo.save()
    assert conn.mock_calls == [call.hmset(key, {'bar_id': '2'})]

    # Assign by setting bar_id
    conn.reset_mock()
    foo.bar_id = 1
    foo.save()
    assert conn.mock_calls == [call.hmset(key, {'bar_id': '1'})]

    # Set to null
    conn.reset_mock()
    foo.bar = None
    foo.save()
    assert conn.mock_calls == [call.hdel(key, 'bar_id')]
