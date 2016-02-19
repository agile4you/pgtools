# -*- coding: utf-8 -*-
"""`pgtools.dbtypes` module.

Provides python binding utilities for postgresql custom types.
"""

__license__ = 'GLPv3'
__date__ = '8-2-2016'
__author__ = 'pav'


from collections import OrderedDict
import six
import weakref
import psycopg2.extensions
import psycopg2.extras

try:
    import ujson as json
except ImportError:
    import json


class ModelError(Exception):
    """Raises when a model filed error occurs.
    """
    pass


class ModelField(object):
    """Base model field.
    """
    def __init__(self):
        self._cache = weakref.WeakKeyDictionary()

    def __get__(self, instance, owner):
        """Descriptor `__get__` method.
        """
        if not instance and owner:
            return self

        return self._cache.get(instance, None)

    def __set__(self, instance, value):
        cleaned_value = self.clean_value(value)
        self._cache[instance] = cleaned_value

    def raise_error(self, value):
        raise ModelError('Invalid value `{}` for {}.'.format(
            value,
            self.__class__.__name__
        ))

    def clean_value(self, value):
        raise NotImplementedError('Must implement `clean_value` method.')


class IntegerModelField(ModelField):
    """IntegerModelField
    """
    def clean_value(self, value):
        try:
            return int(value)
        except ValueError:
            self.raise_error(value)


class FloatModelField(ModelField):
    """FloatModelField
    """
    def clean_value(self, value):
        try:
            return int(value)
        except ValueError:
            self.raise_error(value)


class TextModelField(ModelField):
    """TextField
    """
    def clean_value(self, value):
        return value


class JSONModelField(ModelField):
    """JSONField
    """
    def clean_value(self, value):
        try:
            return json.loads(value.replace("'", '"'))
        except Exception:
            return self.raise_error(value)


class ModelMeta(type):
    """Metaclass for model classes.
    """
    @classmethod
    def __prepare__(mcs, name, bases):
        return OrderedDict()

    def __new__(mcs, name, bases, attrs):
        cls_fields = [key for key, value in attrs.items()
                      if ModelField in value.__class__.mro()]

        def cls_init(self, **kwargs):
            for k, v in kwargs.items():
                if k in self.fields():
                    print(k)
                    setattr(self, k, v)

        def to_dict(self):
            return {attr: getattr(self, attr, None) for attr in self.fields()}

        attrs['__init__'] = cls_init
        attrs['fields'] = classmethod(lambda cls: cls_fields)
        attrs['to_dict'] = property(to_dict)

        return super(ModelMeta, mcs).__new__(mcs, name, bases, attrs)


@six.add_metaclass(ModelMeta)
class BaseModel(object):

    __slots__ = ['__weakref__']

    def __repr__(self):
        return '<{} instance at: 0x{:x}>'.format(
            self.__class__.__name__,
            id(self)
        )

    def __iter__(self):
        for key, value in self.to_dict.items():
            yield key, value

    def __contains__(self, item):
        return item in self.fields()

    def __str__(self):
        return '<{} instance: ({})>'.format(
            self.__class__.__name__,
            ', '.join(['{}={}'.format(k, v) for k, v in self])
        )


def db_cast(model, pg_oid):
    """Postgresql casting utility function

    Args:
        model:
        pg_oid:

    Returns:

    """
    def cast_destination(value, cur=None):
        """A callable that implements `psycopg2.extras.register_type` protocol.
        """
        if not value:
            return None

        python_value = eval(value.replace('""', "'"))
        instance_data = dict(zip(model.fields(), python_value))

        return model(**instance_data)

    mapping_model = psycopg2.extensions.new_type((pg_oid, ), model.__class__.__name__, cast_destination)

    psycopg2.extensions.register_type(mapping_model, None)
