from datetime import datetime

import pytest
from pytz import utc

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

    # Getting non-existent model
    with pytest.raises(DoesNotExist):
        Foo.get(id=2)

    # Test saving without id should fail
    foo = Foo(name='1', num=2)
    with pytest.raises(Exception):
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
