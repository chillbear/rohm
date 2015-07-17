import copy

import six

from rohm import model_registry
from rohm.fields import BaseField, IntegerField, RelatedModelField, RelatedModelIdField
from rohm.connection import get_connection
from rohm.exceptions import DoesNotExist
from rohm.utils import redis_operation

conn = get_connection()


class ModelMetaclass(type):
    def __new__(meta, name, bases, attrs):

        # Figure out name of primary key field
        pk_field = None
        for key, val in attrs.items():
            if isinstance(val, BaseField) and val.is_primary_key:
                pk_field = key
                # break
            if isinstance(val, RelatedModelField):
                # add a id field
                id_field = '{}_id'.format(key)
                attrs[id_field] = RelatedModelIdField(key)

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

        model_registry[name] = cls


class Model(six.with_metaclass(ModelMetaclass)):
    """
    Things on the class (use underscores)
    _pk_field

    _fields: dictionary of Field instances of the class

    Note on terminology:
    - field: a Field object instance.
    ... but sometimes means "field name" as shorthand, and sometimes a field_name/field_val pair
    - field_name: the name of a field
    - field_val: the value of a Model instance's field

    """
    track_modified_fields = True
    save_modified_only = True
    ttl = None

    def __init__(self, _new=True, _partial=False, **kwargs):
        """
        Args:
        ------
        _new: Is this a brand new thing_, or loaded from Redis

        """
        self._data = {}
        self._new = _new
        self._orig_data = {}
        self._loaded_field_names = set()
        self._loaded_related_field_data = {}

        for key, val in kwargs.items():
            if key in self._fields:
                setattr(self, key, val)

        if not _new and not _partial:
            # indicate that all fields are "loaded"
            self._loaded_field_names = set(self._get_field_names())

        # check default vals
        for field_name, field in self._fields.items():
            if field_name not in self._data and field.default:
                # set default value
                default_val = field.get_default_value()
                setattr(self, field_name, default_val)

        if self.track_modified_fields:
            self._reset_orig_data()

    @property
    def pk(self):
        return getattr(self, self._pk_field)

    @classmethod
    def get(cls, pks=None, id=None, fields=None, raise_missing_exception=None):
        # get from redis
        pks = pks or id

        single = not isinstance(pks, (list, tuple))

        if raise_missing_exception is None:
            raise_missing_exception = single

        if single:
            pks = [pks]

        if fields:
            if cls._pk_field not in fields:
                fields.append(cls._pk_field)

        partial = bool(fields)

        pipe = conn.pipeline()
        for pk in pks:
            redis_key = cls.generate_redis_key(pk)
            if partial:
                pipe.hmget(redis_key, fields)
            else:
                pipe.hgetall(redis_key)

        results = pipe.execute()

        instances = []
        for result in results:
            if not result:
                if raise_missing_exception:
                    raise DoesNotExist
                else:
                    continue

            if partial:
                raw_data = {k: v for k, v in zip(fields, result)}
            else:
                raw_data = result

            data = {}
            for k, v in raw_data.items():
                if k in cls._fields:
                    data[k] = cls._convert_field_from_raw(k, v)
            instance = cls(_new=False, _partial=partial, **data)
            instances.append(instance)

        if single:
            return instances[0]
        else:
            return instances

    #
    # @classmethod
    # def get_multiple(cls, pks=None):
    #     pipe = conn.pipeline()
    #     for pk in pks:
    #

    @classmethod
    def set(cls, pk=None, id=None, **data):
        pk = pk or id
        redis_key = cls.generate_redis_key(pk)

        if conn.exists(redis_key):
            raise DoesNotExist

        with redis_operation(conn, pipelined=cls.ttl is not None) as _conn:
            _conn.hmset(redis_key, data)

            if cls.ttl:
                _conn.expire()

    @classmethod
    def _convert_field_from_raw(cls, field_name, raw_val):
        """
        For a given field name and raw data, get a cleaned data value
        """
        field = cls._get_field(field_name)
        cleaned = field.from_redis(raw_val)
        return cleaned

    def _get_field_from_redis(self, field_name):
        redis_key = self.get_redis_key()
        raw = conn.hget(redis_key, field_name)
        cleaned = self._convert_field_from_raw(field_name, raw)
        # cleaned = self._get_field(field_name).from_redis(raw)
        return cleaned

    def _load_field_from_redis(self, field_name):
        val = self._get_field_from_redis(field_name)
        setattr(self, field_name, val)
        return val

    def _load_related_field(self, field_name):
        related_field = self._get_field(field_name)
        id_field_name = self._get_related_id_field_name(field_name)
        id = getattr(self, id_field_name)
        # NOW GET RELATED MODEL
        model_cls = related_field.model_cls
        instance = self._get_related_model_by_pk(model_cls, id)

        self._loaded_related_field_data[field_name] = instance

        return instance

    def _get_related_model_by_pk(self, model_cls, pk):
        """
        Can override this to customize related model fetching (e.g. LiteModel)
        """
        return model_cls.get(pk)

    def _get_related_id_field_name(self, field_name):
        return '{}_id'.format(field_name)

    def get_or_create(self):
        # create in Redis if it doesn't exist
        pass

    # def _get_data_with_fields(self, data=None):
    #     data_with_fields = []
    #     data = data or self._data
    #     for key, val in data.items():
    #         field = self._get_field(key)
    #         data_with_fields.append((key, val, field))
    #     return data_with_fields

    def save(self, force=False, modified_only=False):
        # self.validate()
        modified_only = modified_only or self.save_modified_only

        redis_key = self.get_redis_key()

        if self._new and not force and conn.exists(redis_key):
            raise Exception('Object already exists')

        if modified_only and not self._new:
            modified_data = self._get_modified_fields()
            cleaned_data, none_keys = self.get_cleaned_data(data=modified_data)
        else:
            cleaned_data, none_keys = self.get_cleaned_data()

        if cleaned_data or none_keys:
            use_pipe = cleaned_data and none_keys or self.ttl

            with redis_operation(conn, pipelined=use_pipe) as _conn:
                if cleaned_data:
                    _conn.hmset(redis_key, cleaned_data)

                if none_keys:
                    _conn.hdel(redis_key, *none_keys)

                if self.ttl:
                    _conn.expire(redis_key, self.ttl)

            if self.track_modified_fields:
                self._reset_orig_data()
        else:
            print 'warning no save'
        # now it's been saved
        self._new = False

    def delete(self):
        redis_key = self.get_redis_key()
        conn.delete(redis_key)

    @classmethod
    def _get_field(cls, name):
        return cls._fields[name]

    @classmethod
    def _get_field_names(cls):
        return list(cls._fields.keys())

    def get_cleaned_data(self, data=None, separate_none=True):
        """
        - separate_none: move any None values into a separate list, and return
          (non_none_data, none_keys)
        """
        cleaned_data = {}
        none_keys = []

        if data is None:
            data = self._data
        else:
            data = data or {}

        # for name, val, field in self._get_data_with_fields():
        for name, val in data.items():
            field = self._get_field(name)
            field.validate(val)
            cleaned_val = field.to_redis(val)

            if separate_none and val is None:
                none_keys.append(name)
            else:
                cleaned_data[name] = cleaned_val

        if separate_none:
            return cleaned_data, none_keys
        else:
            return cleaned_data

    def get_redis_key(self):
        pk = getattr(self, self._pk_field)
        if not pk:
            raise Exception('No primary key set!')

        return self.generate_redis_key(pk)

    # def _process_none_values(self, data):
    #     non_empty_data = {}
    #     none_values = []
    #     for key, val in data.items():
    #         if val is None:
    #             none_values.append(key)
    #         else:
    #             non_empty_data[key] = val
    #
    #     return non_empty_data, none_values

    @classmethod
    def generate_redis_key(cls, pk):
        key = '{}:{}'.format(cls._key_prefix, pk)
        return key

    def _reset_orig_data(self):
        """
        Reset _orig_data back
        """
        self._orig_data = copy.deepcopy(self._data)

    def _get_modified_fields(self):
        """
        Get the fields that have changed on the model since loading it
        Works by comparing values, so should work for mutable JSON fields too
        Returns a dictionary of {field_name: new_value}
        """
        fields = {}
        for key, val in self._data.iteritems():
            try:
                if val != self._orig_data[key]:
                    fields[key] = val
            except KeyError:
                fields[key] = val

        return fields

    @property
    def _modified_field_names(self):
        return set(self._get_modified_fields().keys())

    def __repr__(self):
        return '<{}:{}>'.format(self.__class__.__name__, str(self))

    def __str__(self):
        return str(self.pk)
