
import pytest
from mock import call

from rohm.models import Model
from rohm import fields
from rohm.exceptions import DoesNotExist
from rohm.utils import utcnow


def test_simple_model():

    class Foo(Model):
        name = fields.CharField()
        num = fields.IntegerField()
    id = 1
    name = 'foo'
    num = 123
    foo = Foo(id=id, name=name, num=num)

    # Basic attribute access

    def check_attrs(foo):

        assert foo.id == id
        assert foo.name == name
        assert foo.num == num

    check_attrs(foo)
    foo.save()

    foo = Foo.get(id=1)
    check_attrs(foo)

    # Change a field
    foo.num = 456
    foo.save()

    foo = Foo.get(1)
    assert foo.num == 456

    # Getting non-existent model
    with pytest.raises(DoesNotExist):
        Foo.get(id=2)

    # Test saving without id should fail
    foo = Foo(name='1', num=2)
    with pytest.raises(Exception):
        foo.save()


class TestNoneField(object):

    def test_none_field_basics(self, conn):

        """
        Test behavior of allow_none fields
        - reading a field that is None
        - loading an object with a None field, we should distinguish between knowing that a field is
          None, vs not having loaded the field at all
        - try saving a field from None -> something, and vice versa
        """

        class Foo(Model):
            name = fields.CharField()
        return Foo

        foo = Foo(id=1)
        assert foo.name is None
        foo.save()

        redis_key = 'foo:1'

        # should be no calls get (bug with name __get__ calling from Redis)
        assert conn.hmget.call_count == 0

        conn.reset_mock()

        # understand that we already "loaded" that this field is None
        foo = Foo.get(id=1)
        assert conn.mock_calls == [call.hgetall(redis_key)]
        assert foo._loaded_field_names == {'id', 'name'}

        assert foo.name is None   # this should not trigger Redis call

        foo.name = 'asdf'
        assert foo._get_modified_fields() == {'name': 'asdf'}
        foo.save()
        assert conn.mock_calls[-1] == call.hmset(redis_key, {'name': 'asdf'})
        assert conn.hgetall(redis_key) == {'id': '1', 'name': 'asdf'}

        conn.reset_mock()

        # Now try overriding existing value with None! Should do a delete operation
        foo.name = None
        foo.save()
        assert conn.mock_calls == [call.hdel(redis_key, 'name')]
        assert conn.hgetall(redis_key) == {'id': '1'}

    def test_none_field_mixed(self, conn):
        """
        Try saving a real value and a None value at same time
        """
        class Foo(Model):
            a = fields.CharField()
            b = fields.CharField()

        foo = Foo(id=1, a='foo', b='bar')
        foo.save()
        data = conn.hgetall('foo:1')
        assert data == {'id': '1', 'a': 'foo', 'b': 'bar'}

        foo = Foo.get(1)
        foo.a = 'alpha'
        foo.b = None
        foo.save()

        # Should be a pipeline, but that's all we can introspect
        assert conn.mock_calls[-1] == call.pipeline()

        # Check what's in redis
        data = conn.hgetall('foo:1')
        assert data == {'id': '1', 'a': 'alpha'}


@pytest.mark.parametrize('save_modified_only', (False, True))
def test_save_modified_only(save_modified_only, conn):
    class Foo(Model):
        name = fields.CharField()
        num = fields.IntegerField()
    Foo.save_modified_only = save_modified_only

    foo = Foo(id=1, name='foo', num=12)
    foo.save()

    foo = Foo.get(1)

    conn.reset_mock()
    foo.name = 'something'
    foo.save()

    if save_modified_only:
        print conn.mock_calls
        conn.hmset.assert_called_once_with('foo:1', {'name': 'something'})

        # next save should do nothing
        foo.save()
        conn.reset_mock()
        assert conn.mock_calls == []
    else:
        data = {'id': '1', 'name': 'something', 'num': '12'}
        conn.hmset.assert_called_once_with('foo:1', data)

        # other saves will still save, sadly
        conn.reset_mock()
        foo.save()
        conn.hmset.assert_called_once_with('foo:1', data)

def test_datetime_field():
    class DefaultTimeModel(Model):
        created_at = fields.DateTimeField(default=utcnow)

    foo = DefaultTimeModel(id=1)
    foo.save()

    foo = DefaultTimeModel.get(id=1)
    assert foo.created_at
    print foo.created_at

    class SimpleDefaultModel(Model):
        count = fields.IntegerField(default=5)

    foo = SimpleDefaultModel()
    assert foo.count == 5


def test_partial_fields(conn):
    """
    Test that we can selectively load a few fields from Redis, and the unloaded ones will
    get loaded on demand
    """
    class Foo(Model):
        name = fields.CharField()
        num = fields.IntegerField()

    foo = Foo(id=1, name='foo', num=20)
    foo.save()

    foo = Foo.get(id=1, fields=['name'])
    assert foo._loaded_field_names == {'id', 'name'}
    assert conn.hmget.call_count == 1

    conn.reset_mock()

    # access another field
    assert foo.num == 20
    assert conn.hget.call_count == 1
    assert conn.hget.call_args_list == [call('foo:1', 'num')]
    assert foo._loaded_field_names == {'id', 'name', 'num'}

    # access again
    print foo.num
    assert conn.hget.call_count == 1
