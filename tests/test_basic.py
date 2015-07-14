
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


def test_none_field(mockconn):

    class Foo(Model):
        name = fields.CharField()

    foo = Foo(id=1)
    assert foo.name is None
    foo.save()

    # should be no calls get (bug with name __get__ calling from Redis)
    assert mockconn.hmget.call_count == 0

    mockconn.reset_mock()

    foo = Foo.get(id=1)
    assert mockconn.mock_calls == [call.hgetall('foo:1')]
    assert foo._loaded_field_names == {'id', 'name'}

    assert foo.name is None   # this should not trigger Redis call

    foo.name = 'asdf'
    assert foo._get_modified_fields() == {'name': 'asdf'}
    foo.save()
    assert mockconn.mock_calls[-1] == call.hmset('foo:1', {'name': 'asdf'})

    # Now try overriding existing value with None! this is tricky..
    foo.name = None
    # import pdb; pdb.set_trace()

    foo.save()


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


def test_partial_fields(mockconn):

    class Foo(Model):
        name = fields.CharField()
        num = fields.IntegerField()

    foo = Foo(id=1, name='foo', num=20)
    foo.save()

    foo = Foo.get(id=1, fields=['name'])
    assert mockconn.hmget.call_count == 1

    mockconn.reset_mock()

    # access another field
    assert foo.num == 20
    assert mockconn.hget.call_count == 1
    assert mockconn.hget.call_args_list == [call('foo:1', 'num')]

    # access again
    print foo.num
    assert mockconn.hget.call_count == 1
