
import pytest
from mock import call

from rohm.models import Model
from rohm import fields
from rohm.exceptions import DoesNotExist
from rohm.utils import utcnow


def test_fields():

    class Foo(Model):
        name = fields.CharField()
        num = fields.IntegerField()
        comments = fields.JSONField()

    comments = {
        'stuff': 1,
        'whoa': 'cool',
    }

    foo = Foo(
        id=1,
        name='foo',
        num=2,
        comments=comments,
    )
    foo.save()

    foo = Foo.get(1)
    assert foo.name == 'foo'
    assert foo.num == 2
    assert foo.comments == comments


def test_default_fields():
    class SimpleDefaultModel(Model):
        count = fields.IntegerField(default=5)

    foo = SimpleDefaultModel()
    assert foo.count == 5


def test_datetime_field():
    class DefaultTimeModel(Model):
        created_at = fields.DateTimeField(default=utcnow)

    foo = DefaultTimeModel(id=1)
    foo.save()

    foo = DefaultTimeModel.get(id=1)
    assert foo.created_at

def test_float_field():
    class FloatModel(Model):
        x = fields.FloatField()

    int_val = 1
    foo = FloatModel(x=int_val, id=1)
    foo.save()

    assert foo.x == int_val
    foo = FloatModel.get(id=1)
    assert foo.x == int_val
    assert isinstance(foo.x, int)

    float_val = 3.2
    bar = FloatModel(id=2, x=float_val)
    bar.save()

    assert bar.x == float_val
    assert FloatModel.get(id=2).x == float_val
