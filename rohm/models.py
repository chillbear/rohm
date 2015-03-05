import six


class ModelMetaclass(object):
    pass


class Model(six.with_metaclass(ModelMetaclass)):
    pass
