class BaseField(object):
    # _field_name = None  # placeholder

    def __init__(self, primary_key=False, required=False, allow_none=True, *args, **kwargs):
        self.is_primary_key = primary_key
        self.required = required
        # print 'field init', args, kwargs

    def __get__(self, instance, owner):
        val = instance._data.get(self.field_name, None)
        return val

    def __set__(self, instance, value):
        instance._data[self.field_name] = value


class IntegerField(BaseField):
    pass


class CharField(BaseField):
    pass
