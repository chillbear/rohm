class BaseField(object):
    # _field_name = None  # placeholder

    def __init__(self, *args, **kwargs):
        pass
        # print 'field init', args, kwargs

    def __get__(self, instance, owner):
        val = instance._data.get(self._field_name, None)
        return val

    def __set__(self, instance, value):
        instance._data[self._field_name] = value


class IntegerField(BaseField):
    pass


class CharField(BaseField):
    pass
