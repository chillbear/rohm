import pytest
from mock import call

from rohm.models import Model, ROHM_ENABLE_TRANSACTION
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

    if ROHM_ENABLE_TRANSACTION:
        assert pipe.hgetall.call_count == 1
    else:
        assert pipe.hgetall.call_count == 4
    assert pipe.hgetall.call_args == call(key)

    pipe.reset_mock()

    # Check internal stuff
    assert set(foo._data.keys()) == {'id', 'name', 'bar_id'}
    assert foo._loaded_related_field_data == {}
    assert foo._loaded_field_names == {'id', 'name', 'bar_id'}

    # Reading bar_id should not access Redis
    assert foo.bar_id == 1
    assert pipe.hgetall.call_count == 0
    assert pipe.hget.call_count == 0
    assert pipe.hmget.call_count == 0

    # Should only read related field here
    pipe.reset_mock()

    bar = foo.bar
    assert bar.title == 'bar1'
    assert pipe.hgetall.call_args_list == [call('bar:1')]

    # _data should not contain related Bar
    assert set(foo._data.keys()) == {'id', 'name', 'bar_id'}
    assert foo._loaded_related_field_data == {'bar': bar}
    assert foo._loaded_field_names == {'id', 'name', 'bar_id'}

    # Reassign bar
    pipe.reset_mock()
    foo.bar = bar2
    foo.save()
    assert pipe.hmset.call_args_list == [call(key, {'bar_id': '2'})]

    # Assign by setting bar_id
    pipe.reset_mock()
    foo.bar_id = 1
    foo.save()
    assert pipe.hmset.call_args_list == [call(key, {'bar_id': '1'})]

    # Set to null
    pipe.reset_mock()
    foo.bar = None
    foo.save()
    assert pipe.hdel.call_args_list == [call(key, 'bar_id')]


def test_related_none(Foo, Bar):
    foo = Foo(id=1, name='foo')
    foo.save()
    assert foo.bar is None


def test_related_partial(Foo, Bar, conn, pipe):
    """
    Test partial field loading and related..
    """
    bar1 = Bar(id=2, title='bar1')
    bar1.save()

    foo = Foo(id=1, name='foo', bar=bar1)
    foo.save()

    foo = Foo.get(id=1, fields=['name'])
    assert foo._data == dict(id=1, name='foo')

    # Access bar
    assert foo.bar.title == bar1.title

    # internal stuff
    assert foo._data == dict(id=1, name='foo', bar_id=2)
    assert foo._loaded_field_names == {'id', 'name', 'bar_id'}

    # Try loading bar_id separately...?

    foo = Foo.get(id=1, fields=['name'])
    str(foo.bar_id)
    assert foo._data == dict(id=1, name='foo', bar_id=2)

    str(foo.bar)
