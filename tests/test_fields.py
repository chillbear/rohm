
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
