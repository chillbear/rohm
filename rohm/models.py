import six

from rohm.fields import BaseField, IntegerField

from rohm.connection import get_connection


conn = get_connection()


class ModelMetaclass(type):
    def __new__(meta, name, bases, attrs):

        primary_key = None
        for key, val in attrs.items():
            if isinstance(val, BaseField) and val.is_primary_key:
                primary_key = key
                break

        if primary_key is None:
            id_field = IntegerField(primary_key=True)
            primary_key = 'id'
            attrs[primary_key] = id_field

        attrs['_primary_key'] = primary_key
        return super(ModelMetaclass, meta).__new__(meta, name, bases, attrs)

    def __init__(cls, name, bases, attrs):

        super(ModelMetaclass, cls).__init__(name, bases, attrs)

        cls._fields = {}
        # cls._primary_key = None
        for key, val in attrs.items():
            if isinstance(val, BaseField):
                # let field know its name!
                field = val
                val.field_name = field
                cls._fields[key] = field

                # if field.is_primary_key:
                #     cls._primary_key = key


class Model(six.with_metaclass(ModelMetaclass)):

    def __init__(self, **kwargs):
        self._data = {}

        for key, val in kwargs.items():
            if key in self._fields:
                setattr(self, key, val)

    def get(self):
        # get from redis
        pass

    def get_or_create(self):
        # create in Redis if it doesn't exist
        pass

    def save(self):
        # save to redis fool!
        pass
