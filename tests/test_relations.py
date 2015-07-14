from datetime import datetime

from mock import call
import pytest
from pytz import utc

from rohm.models import Model
from rohm import fields
from rohm.exceptions import DoesNotExist
from rohm.utils import utcnow





def test_related(mockconn, mocker):

    class Foo(Model):
        name = fields.CharField()
        bar = fields.RelatedModelField('Bar')

    class Bar(Model):
        title = fields.CharField()

    bar1 = Bar(id=1, title='bar1')
    bar1.save()

    # bar_get = mocker.spy(bar1, 'get')

    bar2 = Bar(id=2, title='bar2')
    bar2.save()

    foo = Foo(id=1, name='foo', bar=bar1)
    foo.save()

    # mockconnection.
    foo = Foo.get(1)
    assert mockconn.hgetall.call_count == 1
    assert mockconn.hgetall.call_args_list == [call('foo:1')]

    mockconn.reset_mock()

    # Should only read related field here
    bar = foo.bar
    assert foo.bar.title == 'bar1'
    assert mockconn.hgetall.call_args_list == [call('bar:1')]


    # import ipdb; ipdb.set_trace()
