# -*- coding: utf-8 -*-
"""`pgtools.dbapi` module.

Provides models and utilities for using Postgresql Database API's
(UDF/Views) with parameter validation.
"""

__license__ = 'GLPv3'
__date__ = '1-9-015'
__author__ = 'pav'

__all__ = ['DBAPIBackend', 'FunctionField', 'ViewField', 'DBAPIError', 'UnknownParamError',
           'InvalidFunctionParamError']

# Try to find the best candidate for JSON serialization.
# Import order indicates serialization efficiency.

import six

try:
    import ujson as json
except ImportError:
    try:
        import simplejson as json
    except ImportError:
        import json


class DBAPIError(Exception):
    """Base module exception
    """
    pass

class InvalidFunctionParamError(DBAPIError):
    """Raises when invalid type parameter is passed to Postgresql Function.
    """
    pass


class BaseFuncArg(object):
    """**Base Postgresql Function parameter validator**

    Implements a base validator template class for function parameters.
    Subclass it and define the attributes.


    Attributes:
        - formatter (str): The string template corresponding to DB native form.
        - param_type (tuple): The parameter native classes allowed.
    """

    formatter = ''
    param_type = None

    def __init__(self, param):
        self.param = param

    def validate(self):
        if not isinstance(self.param, self.param_type):
            raise InvalidFunctionParamError(
                "Invalid type for {} param".format(self.param_type)
            )
        return self.formatter.format(
            self.format()
        )

    def format(self):
        return self.param


class BaseAPIField(object):
    """**Base ModelField Descriptor class**

    Model Field Descriptor Abstract class. It provides the minimum interface
    for building Model class attributes that map to Database object. It is not
    intented to be initialized, although won't raise a ``TypeError``.

    You need to subclass it and override at least ``__get__`` special method
    or add any method you like. BaseField encapsulates an inner functionality
    that subclasses need. For now its only ``auto-tagging``.

    Attributes:
        - field (str): The descriptor name for instance owner class.
        - lazyload (boolean): Indicates if result should be cached on instance.

    .. note::
        BaseField will also implement ``lazyload`` functionality. So if you
        really need to override ``__init__`` special method don't forget to
        call ``super`` (as shown in example below).

    Subclass example::

        >>> class MyCustomField(BaseAPIField):
        ...     def __init__(self, my_value, **kwargs):
        ...         self.custom_attr = my_value
        ...         super(MyCustomField, self).__init__(**kwargs)
        ...
        ...     def __get__(self, instance, owner):
        ...         return instance.some_attr is self.custom_attr
        ...

    """

    def __init__(self, *args, **kwargs):
        self.args = args
        self.field = None
        self.lazyload = kwargs.get('cache') or False

    def __get__(self, instance, owner):
        """Implementing descriptor '__get__' method.
        """
        raise NotImplementedError


class NumArg(BaseFuncArg):
    """Numeric Parameters formatter/validator class.
    """
    formatter = '{}'
    param_type = (int, float)


class TextArg(BaseFuncArg):
    """Textual Parameters formatter/validator class.
    """
    formatter = "'{}'"
    param_type = (six.string_types, )


class ArrayArg(BaseFuncArg):
    """Array Parameters formatter/validator class.
    """
    formatter = "'{}'"
    param_type = (list, tuple)

    def format(self):
        array_data = str(self.param)[1:-1].replace("u'", "").replace("'", '')
        return '{%s}' % array_data


class JSONArg(BaseFuncArg):
    """JSON Parameters formatter/validator class.
    """
    formatter = "'{}'"
    param_type = (dict, )

    def format(self):
        return json.dumps(self.param)


FUNC_TYPES = {
    int: NumArg,
    float: NumArg,
    str: TextArg,
    dict: JSONArg,
    list: ArrayArg,
    tuple: ArrayArg
}


class UnknownParamError(DBAPIError):
    """Raises when passing unknown parameter in Postgresql function.
    """
    pass


def param_validator(param_specs, params, order=None):
    """Parameter validator function.

    >>> param_specs = {"email": str, "tags": list}
    >>> params = {"email": "pav@gmail.com", "tags": [1, 2, 3, 4, 5]}
    >>> print(', '.join(sorted(param_validator(param_specs, params))))
    'pav@gmail.com', '{1, 2, 3, 4, 5}'
    """
    if not set(param_specs).issuperset(set(params)) or not params:
        raise UnknownParamError("Invalid function arguments: {} - {}".format(
            set(param_specs), set(params)
        ))

    if not order:
        return (FUNC_TYPES[param_specs[key]](params[key]).validate()
                for key in params)

    return (FUNC_TYPES[param_specs[key]](params[key]).validate()
            for key in order)


class ViewField(BaseAPIField):
    """**ViewField Descriptor class**

    ViewField is non-data Descriptor that subclasses
    :class:`pgtools.dbapi.BaseAPIField` and wraps a Database schema view object
    .It should be used with no initial arguments at the meantime (as show in
    example below).

    .. note::
        ViewField class can be initialized with arguments, but so far they will
        have no effect. The only reason that it is validated is that soon you
        could pass parameters that control ``cache_property`` behavior and
        result pagination. Classes with ``ViewField`` attributes *must* have
        a `persistence` attribute that implements
        `pg_utils.bases.BaseConnectionPool`interface.

    Example usage::

        >>> class MyClass(object):
        ...     test_function = ViewField()
        ...
    """

    def __get__(self, instance, owner):
        """Implementing descriptor '__get__' method.
        """
        if not instance and owner:
            return self

        def _callback():
            return  'SELECT * FROM {};'.format(self.field)

        return _callback


class FunctionField(BaseAPIField):
    """**FunctionField Descriptor class**

    FunctionField is also a non-data descriptor class that subclasses
    :class:`pgtools.dbapi.BaseAPIField` for wrapping database schema functions.

    You must initialize it with python native types that
    correspond to Database function parameter types (as you example below).
    The ``__get__`` method returns a callable object, that acts as the actual
    database function with parameter validation.

    .. note::
        FunctionField can also be initialized with extra parameters with no
        effect for the meantime. When all functionality is enabled you should
        explicitly declare kwargs, for easier debugging. The class that has
        ``FunctionField`` attributes *must* have a `persistence` attribute
        that implements `pg_utils.bases.BaseConnectionPool`interface.


    Example Usage (assuming there is a db function with that name and param)::

        >>> class MyModel(object):
        ...     get_model_by_pk = FunctionField(pk=int)
        ...
    """

    def __init__(self, order=None, **func_params):
        self.order = order
        self.func_specs = func_params
        super(FunctionField, self).__init__(**func_params)

    def __get__(self, instance, owner):
        """Implementing descriptor '__get__' method.
        """
        if not instance and owner:
            return self

        def func_callable(**args):

            function_query = 'SELECT * FROM {}({});'.format(
                self.field,
                ', '.join(param_validator(
                    self.func_specs, args, self.order
                ))
            )

            return function_query

        return func_callable


class DBAPIMeta(type):
    """Base metaclass for DBAPIBackend classes.

    ModelMeta ensures two things.

    1] Initialized Classes implement `Singleton` pattern.
    2] ViewFields, and FunctionFields auto tagging from class
       labeling.
    """
    _instances = {}

    def __new__(mcs, name, bases, attrs):

        cls_meta = attrs.get('Meta')
        cls_schema = getattr(cls_meta, 'schema', None)
        for cls_tag, attr in attrs.items():
            try:
                if BaseAPIField in attr.__class__.__mro__:
                    attr.field = "{}.{}".format(
                        cls_schema or 'public',
                        cls_tag
                    )
                    attr.tag = cls_tag
            except AttributeError:
                pass
        return super(DBAPIMeta, mcs).__new__(mcs, name, bases, attrs)

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(
                DBAPIMeta,
                cls
            ).__call__(*args)
        return cls._instances[cls]


@six.add_metaclass(DBAPIMeta)
class DBAPIBackend(object):
    """**Abstract Base DBAPIBackend class**

    This is the base model class for implementing data models that correspond
    to Database namespace objects. Derivatives from BaseModel in order to use

    Subclasses implement by default the *Singleton* pattern (that means that
    only an instance is created) and also enables BaseField ``auto-tagging``
    functionality.

    Example Usage::

        >>> class ProductModel(DBAPIBackend):
        ...     # declare `function` API
        ...     get_product = FunctionField(pk=int)
        ...     create_product = FunctionField(product_name=str)
        ...
        ...     # declare `view` API
        ...     latest_products = ViewField()
        ...     count_products = ViewField()
        ...
        ...     class Meta:
        ...         schema = 'product'
        ...
        >>> product = ProductModel()
        >>> product.count_products()
        'SELECT * FROM product.count_products;'
        >>> product.get_product(pk=6526352)
        'SELECT * FROM product.get_product(6526352);'


    """


if __name__ == '__main__':
    import doctest
    doctest.testmod()
