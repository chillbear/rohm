import datetime
from dateutil.parser import parse as dateparse
import types
import json

import six

# from rohm.exceptions import FieldValidationError
from rohm import model_registry
import pytz
from pytz import utc


class BaseField(object):
    allowed_types = None

    def __init__(self, primary_key=False, required=False, allow_none=True, default=None,
                 *args, **kwargs):
        self.is_primary_key = primary_key
        self.required = required
        self.allow_none = allow_none
        self.default = default

        self.field_name = None   # needs to be set

    def __get__(self, instance, owner):
        field_name = self.field_name

        if not instance._new and not self.is_primary_key and field_name not in instance._loaded_field_names:
            return instance._load_field_from_redis(field_name)

        val = instance._data.get(field_name, None)
        return val

    def __set__(self, instance, value):
        field_name = self.field_name
        instance._data[field_name] = value
        instance._loaded_field_names.add(field_name)

    def get_default_value(self):
        if isinstance(self.default, types.FunctionType):
            val = self.default()
        else:
            val = self.default

        return val

    # this is the actual function called
    def to_redis(self, val):
        if self.allow_none and val is None:
            return None
        else:
            return self._to_redis(val)

    def from_redis(self, val):
        if self.allow_none and val is None:
            return None
        else:
            return self._from_redis(val)

    def validate(self, val):

        if self.allow_none and val is None:
            return

        # if 'allowed_types' is set then enforce it
        if self.allowed_types and not isinstance(val, self.allowed_types):
            raise Exception('{} not in allowed types ({})'.format(val, self.allowed_types))

        self._validate(val)

    def _validate(self, val):
        pass

    def _to_redis(self, val):
        return str(val)

    def _from_redis(self, val):
        return val


class IntegerField(BaseField):
    allowed_types = six.integer_types

    def _to_redis(self, val):
        return str(val)

    def _from_redis(self, val):
        return int(val)


class CharField(BaseField):
    allowed_types = six.string_types


class BooleanField(BaseField):
    allowed_types = bool

    def _to_redis(self, val):
        return '1' if val else '0'

    def _from_redis(self, val):
        return bool(int(val))


class JSONField(BaseField):
    allowed_types = (dict, list, tuple)

    # preprocess?
    encoder = json.JSONEncoder

    def _to_redis(self, val):
        return json.dumps(val, cls=self.encoder)

    def _from_redis(self, val):
        return json.loads(val)


class DateTimeField(BaseField):
    allowed_types = datetime.datetime

    def _to_redis(self, val):

        if not val.tzinfo:
            print 'Warning, timezone'
            val = utc.localize(val)

        else:
            val = val.astimezone(pytz.utc)

        val = val.replace(tzinfo=None)
        formatted = val.isoformat()
        return formatted

    def _from_redis(self, val):
        dt = dateparse(val)
        dt = utc.localize(dt)

        return dt


class RelatedModelField(BaseField):
    def __init__(self, model_cls, *args, **kwargs):
        super(RelatedModelField, self).__init__(*args, **kwargs)

        self._model_cls = model_cls

    @property
    def model_cls(self):
        # replace string with the actual model
        if isinstance(self._model_cls, six.string_types):
            self._model_cls = model_registry[self._model_cls]

        return self._model_cls

    # def _get_id_field(self, instance):
    #     return instance._get_field('{}_id'.format(self.field_name))

    def __get__(self, instance, owner):
        field_name = self.field_name

        if field_name not in instance._loaded_related_field_data:
            instance._load_related_field(field_name)

        val = instance._loaded_related_field_data.get(field_name, None)
        return val

    def __set__(self, instance, value):
        # equiv of doing 'driver_id = 5'

        # set the ID field
        id_field_name = instance._get_related_id_field_name(self.field_name)

        id_value = None if value is None else value.id

        setattr(instance, id_field_name, id_value)

        # also set object, of course
        instance._loaded_related_field_data[self.field_name] = value


class RelatedModelIdField(IntegerField):
    def _get_model_field(self, instance):
        return instance._get_field(self.model_field_name)

    def __init__(self, model_field_name, *args, **kwargs):
        # the actual RelatedModelField this corresponds to..
        super(RelatedModelIdField, self).__init__(*args, **kwargs)
        self.model_field_name = model_field_name

    # TODO unset the model when we change this..
    def __set__(self, instance, value):
        super(RelatedModelIdField, self).__set__(instance, value)

        if self.model_field_name in instance._loaded_related_field_data:
            del instance._loaded_related_field_data[self.model_field_name]
