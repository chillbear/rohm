import six

from rohm.fields import BaseField, IntegerField

from rohm.connection import get_connection
from rohm.exceptions import DoesNotExist

conn = get_connection()


class ModelMetaclass(type):
    def __new__(meta, name, bases, attrs):

        # Figure out name of primary key field
        pk_field = None
        for key, val in attrs.items():
            if isinstance(val, BaseField) and val.is_primary_key:
                pk_field = key
                break

        if pk_field is None:
            id_field = IntegerField(primary_key=True)
            pk_field = 'id'
            attrs[pk_field] = id_field

        attrs['_pk_field'] = pk_field
        return super(ModelMetaclass, meta).__new__(meta, name, bases, attrs)

    def __init__(cls, name, bases, attrs):

        super(ModelMetaclass, cls).__init__(name, bases, attrs)

        cls._key_prefix = name.lower()

        cls._fields = {}
        for key, val in attrs.items():
            if isinstance(val, BaseField):
                # let field know its name!
                field = val
                val.field_name = key
                cls._fields[key] = field


class Model(six.with_metaclass(ModelMetaclass)):
    """
    Things on the class (use underscores)
    _pk_field
    """

    def __init__(self, _new=True, **kwargs):
        """
        Args:
        ------
        _new: Is this a brand new thing, or loaded from Redis

        """
        self._data = {}
        self._new = _new

        for key, val in kwargs.items():
            if key in self._fields:
                setattr(self, key, val)

    @classmethod
    def get(cls, pk=None, id=None):
        # get from redis
        pk = pk or id
        redis_key = cls.generate_redis_key(pk)
        raw_data = conn.hgetall(redis_key)

        if raw_data:
            data = {}
            for k, v in raw_data.items():
                if k in cls._fields:
                    field = cls._fields[k]
                    data[k] = field.from_redis(v)
            return cls(new=False, **data)
        else:
            raise DoesNotExist

    def get_or_create(self):
        # create in Redis if it doesn't exist
        pass

    def save(self):
        redis_key = self.get_redis_key()
        print 'save to', redis_key
        conn.hmset(redis_key, self._data)

    def get_redis_key(self):
        pk = getattr(self, self._pk_field)
        if not pk:
            raise Exception('No primary key set!')

        return self.generate_redis_key(pk)

    @classmethod
    def generate_redis_key(cls, pk):
        key = '{}:{}'.format(cls._key_prefix, pk)
        return key
