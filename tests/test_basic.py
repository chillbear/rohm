from datetime import datetime

import pytest
from pytz import utc

from rohm.models import Model
from rohm import fields
from rohm.exceptions import DoesNotExist


def test_simple_model():

    class Foo(Model):
        name = fields.CharField()
        num = fields.IntegerField()
        created_at = fields.DateTimeField()

    created_at = utc.localize(datetime.now())
    id = 1
    name = 'foo'
    num = 123
    foo = Foo(id=id, name=name, num=num, created_at=created_at)

    # Basic attribute access

    def check_attrs(foo):

        assert foo.id == id
        assert foo.name == name
        assert foo.num == num
        assert foo.created_at == created_at

    check_attrs(foo)
    foo.save()

    foo = Foo.get(id=1)
    check_attrs(foo)

    with pytest.raises(DoesNotExist):
        Foo.get(id=2)
