class BaseField(object):
    # _field_name = None  # placeholder

    def __init__(self, *args, **kwargs):
        print 'field init', args, kwargs

    def __get__(self, instance, owner):
        print 'get', instance, owner
        val = instance._data.get(self._field_name, None)
        return val
        # return 123

    def __set__(self, instance, value):
        print 'set', instance, value
        instance._data[self._field_name] = value


class IntegerField(BaseField):
    pass


class CharField(BaseField):
    pass
