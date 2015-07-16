
from mock import call

from rohm.models import Model
from rohm import fields


def test_related(conn, mocker):

    class Foo(Model):
        name = fields.CharField()
        bar = fields.RelatedModelField('Bar')

    class Bar(Model):
        title = fields.CharField()

    bar1 = Bar(id=1, title='bar1')
    bar1.save()

    bar2 = Bar(id=2, title='bar2')
    bar2.save()

    foo = Foo(id=1, name='foo', bar=bar1)
    foo.save()

    foo = Foo.get(1)
    assert conn.hgetall.call_count == 1
    assert conn.hgetall.call_args_list == [call('foo:1')]

    conn.reset_mock()

    # Reading bar_id should not access Redis
    assert foo.bar_id == 1
    assert conn.mock_calls == []

    # Should only read related field here
    bar = foo.bar
    assert bar.title == 'bar1'
    assert conn.hgetall.call_args_list == [call('bar:1')]

    # Reassign bar
    conn.reset_mock()
    foo.bar = bar2
    foo.save()
    assert conn.mock_calls == [call.hmset('foo:1', {'bar_id': '2'})]
