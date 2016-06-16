import copy
import logging

import six
import redis

from rohm import model_registry
from rohm.fields import BaseField, IntegerField, RelatedModelField, RelatedModelIdField
from rohm.connection import get_default_connection, create_connection
from rohm.exceptions import AlreadyExists, DoesNotExist
from rohm.utils import redis_operation, hmget_result_is_nonexistent


logger = logging.getLogger(__name__)


class ModelMetaclass(type):
    def __new__(meta, name, bases, attrs):

        # Figure out name of main "id" field (i.e. "primary key")
        id_field_name = None
        for key, val in attrs.items():
            if isinstance(val, BaseField) and val.is_primary_key:
                id_field_name = key
            if isinstance(val, RelatedModelField):
                # Generate the actual ID field for a relation, suffixed with '_id'
                related_id_field_name = '{}_id'.format(key)
                attrs[related_id_field_name] = RelatedModelIdField(key)

        if id_field_name is None:
            # Make an id_field if not specified
            id_field = IntegerField(primary_key=True)
            id_field_name = 'id'
            attrs[id_field_name] = id_field

        # Store name of our id_field
        attrs['_id_field_name'] = id_field_name

        return super(ModelMetaclass, meta).__new__(meta, name, bases, attrs)

    def __init__(cls, name, bases, attrs):

        super(ModelMetaclass, cls).__init__(name, bases, attrs)

        cls._key_prefix = name.lower()

        # Store a dict of {field_name: Field object}
        cls._fields = {}
        cls._real_fields = {}     # track "real" fields (not RelatedModelField)
        for key, val in attrs.items():
            if isinstance(val, BaseField):
                field = val
                field.field_name = key                # Let field know its own name
                cls._fields[key] = field

                if not isinstance(val, RelatedModelField):
                    cls._real_fields[key] = val

        # Track this Model in a global registry
        model_registry[name] = cls


class Model(six.with_metaclass(ModelMetaclass)):
    """
    Things on the class (use underscores)
    _id_field_name

    _fields: dictionary of Field instances of the class

    Note on terminology:
    - field: a Field object instance.
    ... but sometimes means "field name" as shorthand, and sometimes a field_name/field_val pair
    - field_name: the name of a field
    - field_val: the value of a Model instance's field

    Class variables:
    - track_modified_fields - Track what fields are modified (by storing the original)
    - save_modified_only - On save, only save modified fields. Assumes track_modified_fields==True
    - ttl - Time to live in seconds (uses Redis' built-in ttl)
    """
    track_modified_fields = True
    save_modified_only = True
    ttl = None
    connection = None

    def __init__(self, _new=True, _partial=False, **field_data):
        """
        Args:
        ------
        _new: Is this a brand new thing_, or loaded from Redis
        _partial: Means that only a subset of fields returned

        Instance variables:
        -------------------
        _data: Dictionary of {field_name: field_value}. Only real fields
        _loaded_field_names: A set of fields that have been loaded from Redis. Only includes
                             "real fields" (not RelatedModelField's)
        _loaded_related_field_data: Stores any loaded related Models (from: RelatedModelField)
        """
        self._data = {}
        self._new = _new
        self._orig_data = {}                   # the original data
        self._loaded_field_names = set()       # only for "real" fields, fields that have been loaded
        self._loaded_related_field_data = {}   # for Related stuff

        for key, val in field_data.items():
            # Populate self._data and self._loaded_related_field_data
            # field_data can be both real fields and Models (RelateModelField)
            if key in self._fields:
                setattr(self, key, val)

        if not _new and not _partial:
            # If fully loaded from Redis, indicate that all fields are "loaded"
            self._loaded_field_names = set(self._get_real_field_names())

        # Check default vals (avoid for RelatedModelField though)
        for field_name, field in self._real_fields.items():

            if field_name not in self._data:
                # Handle missing values
                if field.default:
                    # Set default value
                    default_val = field.get_default_value()
                    setattr(self, field_name, default_val)
                elif not _partial and field.allow_none:
                    # None is represented as a lack of a key in Redis, so but
                    # better to expliclty set {'key': None} in self._data
                    setattr(self, field_name, None)

        if self.track_modified_fields:
            self._reset_orig_data()

    # ------------
    # Classmethods
    # ------------
    @classmethod
    def get(cls, ids=None, id=None, fields=None, allow_create=False, raise_missing_exception=None):
        """
        Get a rohm Model from Redis. Can specify one ID or multiple

        - fields: A list/tuple to only load these fields (partial load)
        - allow_create: If missing, allows it to be created. "create_from_id()" must be implemented
        - raise_missing_exception: If missing, raise an exception, otherwise return None
        """
        conn = cls.get_connection()
        ids = id or ids

        assert ids is not None

        if not ids:
            return []

        single = not isinstance(ids, (list, tuple))

        if raise_missing_exception is None:
            raise_missing_exception = single

        if single:
            ids = [ids]

        if fields:
            if cls._id_field_name not in fields:
                # Should also fetch the ID field too..
                fields.append(cls._id_field_name)

        partial = bool(fields)

        pipe = conn.pipeline()
        for id in ids:
            redis_key = cls.generate_redis_key(id)
            if partial:
                pipe.hmget(redis_key, fields)
            else:
                pipe.hgetall(redis_key)

        results = pipe.execute()

        instances = []
        for id, result in zip(ids, results):
            if partial:
                # HMGET returns list of Nones for non-existent key
                exists = not hmget_result_is_nonexistent(result)
            else:
                # Check for truthy (not {} or None)
                exists = bool(result)

            if not exists:
                if allow_create:
                    # If missing, try to create new model
                    try:
                        instance = cls.create_from_id(id)
                        instances.append(instance)
                    except:
                        logger.warning('Could not create object from id %s', id)
                        instances.append(None)
                elif raise_missing_exception:
                    raise DoesNotExist
                else:
                    instances.append(None)
                    continue

            # Dictionary of {field_name --> raw redis data}
            if partial:
                raw_data = {k: v for k, v in zip(fields, result)}
            else:
                raw_data = result

            # Convert to native Python objects
            data = {}
            for k, v in raw_data.items():
                if k in cls._fields:
                    data[k] = cls._convert_field_from_raw(k, v)

            # Create the Model instance
            instance = cls(_new=False, _partial=partial, **data)
            instances.append(instance)

        if single:
            return instances[0]
        else:
            return instances

    @classmethod
    def set(cls, id=None, **data):
        """
        Write to a Model without fetching first
        """
        conn = cls.get_connection()
        redis_key = cls.generate_redis_key(id)

        if conn.exists(redis_key):
            raise DoesNotExist

        with redis_operation(conn, pipelined=cls.ttl is not None) as _conn:
            _conn.hmset(redis_key, data)

            if cls.ttl:
                _conn.expire()

    @classmethod
    def create_from_id(cls, id):
        """
        Subclass should implement for allow_create=True
        """
        raise NotImplementedError

    @classmethod
    def generate_redis_key(cls, id):
        key = '{}:{}'.format(cls._key_prefix, id)
        return key

    @classmethod
    def set_connection(cls, connection):
        """
        Configure the Redis connection for this model
        """
        cls.connection = connection

    @classmethod
    def set_connection_settings(cls, **settings):
        cls.connection = create_connection(**settings)

    @classmethod
    def get_connection(cls):
        return cls.connection or get_default_connection()

    @classmethod
    def _convert_field_from_raw(cls, field_name, raw_val):
        """
        For a given field name and raw data, get a cleaned data value (as Python object)
        """
        field = cls._get_field(field_name)
        cleaned = field.from_redis(raw_val)
        return cleaned

    @classmethod
    def _get_field(cls, name):
        return cls._fields[name]

    @classmethod
    def _get_field_names(cls):
        return list(cls._fields.keys())

    @classmethod
    def _get_real_field_names(cls):
        return list(cls._real_fields.keys())

    # ----------
    # Properties
    # ----------
    @property
    def _id(self):
        """ Return the id value (i.e. primary key) """
        return getattr(self, self._id_field_name)

    @property
    def redis_key(self):
        return self.get_redis_key()

    def get_redis_key(self):
        """ The Redis key (prefix:id) """
        id = getattr(self, self._id_field_name)
        if not id:
            raise Exception('No primary key set!')

        return self.generate_redis_key(id)

    def save(self, modified_only=False, force_create=False, pipe=None):
        """
        Save model to Redis. Will create new one if it doesn't exist

        - force_create: Save if we created a new instance but already exists in Redis
        - modified_only: Only save modified fields
        """
        conn = self.get_connection()

        modified_only = modified_only or self.save_modified_only

        redis_key = self.get_redis_key()

        modified_data = None

        if modified_only and not self._new:
            modified_data = self._get_modified_fields()
            cleaned_data, none_keys = self.get_cleaned_data(data=modified_data)
        else:
            cleaned_data, none_keys = self.get_cleaned_data()

        if pipe is not None:
            is_shared_pipeline = True
        else:
            pipe = conn.pipeline()
            is_shared_pipeline = False

        if cleaned_data or none_keys:
            try:
                if self._new and not force_create and not is_shared_pipeline:
                    # For a new model, use WATCH to detect if someone else wrote to
                    # this key in the meantime. This also puts us in normal execution mode
                    # Don't do this for a multi-object pipelined save
                    pipe.watch(redis_key)

                    exists = pipe.exists(redis_key)
                    if exists:
                        pipe.reset()
                        raise AlreadyExists

                    # Return to buffered MULTI mode
                    pipe.multi()

                if cleaned_data:
                    pipe.hmset(redis_key, cleaned_data)

                if none_keys:
                    # Delete None values
                    pipe.hdel(redis_key, *none_keys)

                if self.ttl:
                    pipe.expire(redis_key, self.ttl)

                # Custom save hook
                self.on_save(conn, modified_data=modified_data)

                if not is_shared_pipeline:
                    pipe.execute()
            except redis.WatchError:
                pipe.reset()
                raise AlreadyExists
            except:
                # We only need to do pipe.reset() for exceptions, so putting it in except
                # rather than in a finally block. (For a shared transaction save() we want to
                # NOT call .reset() or the .command_stack will be cleared)
                pipe.reset()
                raise

            if self.track_modified_fields:
                self._reset_orig_data()
        else:
            pass

        self._new = False

    def delete(self):
        conn = self.get_connection()

        redis_key = self.get_redis_key()

        with redis_operation(conn, pipelined=True) as _conn:
            _conn.delete(redis_key)
            self.on_delete(conn=_conn)

    def on_save(self, conn, modified_data=None):
        """ User-specified save code """
        pass

    def on_delete(self, conn):
        """ User-specified delete code """
        pass

    def get_cleaned_data(self, data=None, separate_none=True):
        """
        Clean data to prepare to send to Redis.
        TODO: maybe rename this function

        - separate_none: move any None values into a separate list, and return
          (non_none_data, none_keys)
        """
        cleaned_data = {}
        none_keys = []

        if data is None:
            data = self._data
        else:
            data = data or {}

        for name, val in data.items():
            field = self._get_field(name)
            field.validate(val)
            cleaned_val = field.to_redis(val)

            if separate_none and cleaned_val is None:
                none_keys.append(name)
            else:
                cleaned_data[name] = cleaned_val

        if separate_none:
            return cleaned_data, none_keys
        else:
            return cleaned_data

    def reload(self):
        """ Reload from Redis """
        return self.get(id=self.id)

    # ---------------
    # Private helpers
    # ---------------
    def _get_field_from_redis(self, field_name):
        conn = self.get_connection()

        redis_key = self.get_redis_key()
        raw = conn.hget(redis_key, field_name)
        cleaned = self._convert_field_from_raw(field_name, raw)
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
        if id:
            model_cls = related_field.model_cls
            instance = self._get_related_model_by_id(model_cls, id)
        else:
            # Handle case of no id
            instance = None

        self._loaded_related_field_data[field_name] = instance
        return instance

    def _get_related_model_by_id(self, model_cls, id):
        """
        Can override this to customize related model fetching (e.g. LiteModel)
        """
        return model_cls.get(id, allow_create=True, raise_missing_exception=False)

    def _get_related_id_field_name(self, field_name):
        return '{}_id'.format(field_name)

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

    def _get_modified_field_names(self):
        return self._get_modified_fields().keys()

    def __repr__(self):
        return '<{}:{}>'.format(self.__class__.__name__, str(self))

    def __str__(self):
        return str(self._id)
