
from mock import call

from rohm.models import Model
from rohm import fields


def test_related(conn, pipe):

    class Foo(Model):
        name = fields.CharField()
        bar = fields.RelatedModelField('Bar')

    class Bar(Model):
        title = fields.CharField()

    key = 'foo:1'

    bar1 = Bar(id=1, title='bar1')
    bar1.save()

    bar2 = Bar(id=2, title='bar2')
    bar2.save()

    foo = Foo(id=1, name='foo', bar=bar1)
    foo.save()

    foo = Foo.get(1)
    assert pipe.hgetall.call_count == 1
    assert pipe.hgetall.call_args[0][1:] == (key,)

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
