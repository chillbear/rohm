import datetime
from dateutil.parser import parse as dateparse

import six

from rohm.exceptions import FieldValidationError
import pytz
from pytz import utc


class BaseField(object):
    # _field_name = None  # placeholder
    allowed_types = None

    def __init__(self, primary_key=False, required=False, allow_none=True, *args, **kwargs):
        self.is_primary_key = primary_key
        self.required = required
        self.allow_none = allow_none

    def __get__(self, instance, owner):
        field_name = self.field_name

        if not self.is_primary_key and field_name not in instance._loaded_fields:
            return instance._load_field_from_redis(field_name)

        val = instance._data.get(field_name, None)
        return val

    def __set__(self, instance, value):
        field_name = self.field_name
        instance._data[field_name] = value
        instance._loaded_fields.add(field_name)

    # this is the actual function called
    def to_redis(self, val):
        return self._to_redis(val)

    def from_redis(self, val):
        if self.allow_none and val is None:
            return None
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
    #     if not isinstance(val, self.allowed_types):
    #         raise FieldValidationError('Incorrect type')

    def _to_redis(self, val):
        return str(val)

    def _from_redis(self, val):
        return val


class IntegerField(BaseField):
    allowed_types = six.integer_types
    # def _validate(self, val):
    #     if not isinstance(val, six.integer_types):
    #         raise FieldValidationError('Not an integer type!')

    def _to_redis(self, val):
        return str(val)

    def _from_redis(self, val):
        return int(val)


class CharField(BaseField):
    allowed_types = six.string_types


class BooleanField(BaseField):
    allowed_types = bool

    # def _validate(self, val):
    #     if not isinstance(val, bool):
    #         raise FieldValidationError('Not an boolean type!')

    def _to_redis(self, val):
        return '1' if val else '0'

    def _from_redis(self, val):
        return bool(int(val))


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
        # if not dt.tzinfo:
        #     print 'Warning, no tzinfo'

        dt = utc.localize(dt)

        return dt
