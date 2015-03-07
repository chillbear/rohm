import six

from rohm.exceptions import FieldValidationError


class BaseField(object):
    # _field_name = None  # placeholder
    def __init__(self, primary_key=False, required=False, allow_none=True, *args, **kwargs):
        self.is_primary_key = primary_key
        self.required = required
        self.allow_none = allow_none

    def __get__(self, instance, owner):
        field_name = self.field_name

        if field_name not in instance._loaded_fields:
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
        return self._from_redis(val)

    def validate(self, val):
        if self.allow_none and val is None:
            return

        self._validate(val)

    def _to_redis(self, val):
        return str(val)

    def _from_redis(self, val):
        return val

    def _validate(self, val):
        pass


class IntegerField(BaseField):
    def _validate(self, val):
        if not isinstance(val, six.integer_types):
            raise FieldValidationError('Not an integer type!')

    def _to_redis(self, val):
        return str(val)

    def _from_redis(self, val):
        return int(val)


class CharField(BaseField):
    pass
