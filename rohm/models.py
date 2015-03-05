import six

from rohm.fields import BaseField


class ModelMetaclass(type):
    # def __new__(cls, name, bases, attrs):
    #     print cls, name, bases, attrs
    #     return super(ModelMetaclass, cls).__new__(cls, name, bases, attrs)

    def __init__(cls, name, bases, attrs):

        super(ModelMetaclass, cls).__init__(name, bases, attrs)

        cls._fields = {}
        for key, val in attrs.items():
            if isinstance(val, BaseField):
                # let field know its name!
                val._field_name = key
                cls._fields[key] = val


class Model(six.with_metaclass(ModelMetaclass)):
    def __init__(self, **kwargs):
        self._data = {}
        for key, val in kwargs.items():
            if key in self._fields:
                setattr(self, key, val)
