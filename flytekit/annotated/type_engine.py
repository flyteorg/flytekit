import datetime as _datetime
from typing import Dict, Tuple, OrderedDict
import collections
from keyword import iskeyword as _iskeyword

from flytekit.common.types import primitives as _primitives
from flytekit.models import types as _type_models
from flytekit.models import interface as _interface_model
from operator import itemgetter as _itemgetter

import sys as _sys
import abc as _abc
import six as _six

# This is now in three different places. This here, the one that the notebook task uses, and the main in that meshes
# with the engine loader. I think we should get rid of the loader (can add back when/if we ever have more than one).
# All three should be merged into the existing one.
SIMPLE_TYPE_LOOKUP_TABLE: Dict[type, _type_models.LiteralType] = {
    int: _primitives.Integer.to_flyte_literal_type(),
    float: _primitives.Float.to_flyte_literal_type(),
    bool: _primitives.Boolean,
    _datetime.datetime: _primitives.Datetime.to_flyte_literal_type(),
    _datetime.timedelta: _primitives.Timedelta.to_flyte_literal_type(),
    str: _primitives.String.to_flyte_literal_type(),
    dict: _primitives.Generic.to_flyte_literal_type(),
}

def type_to_literal_type(t:type) -> _type_models.LiteralType:
    literal_type = SIMPLE_TYPE_LOOKUP_TABLE[t]
    return literal_type

class Outputs(_six.with_metaclass(_abc.ABCMeta, object)):
    @_abc.abstractmethod
    def typed_outputs(self) -> Dict[str, type]:
        pass

def outputs(**kwargs):
    # for k, v in kwargs.items():
    #     kwargs[k] = _interface_model.Variable(
    #         type_to_literal_type(v),
    #         ''
    #     )  # TODO: Support descriptions
    
    # return collections.namedtuple('Outputs', kwargs)
    """Returns a new subclass of tuple with named fields.

    >>> Point = namedtuple('Point', ['x', 'y'])
    >>> Point.__doc__                   # docstring for the new class
    'Point(x, y)'
    >>> p = Point(11, y=22)             # instantiate with positional args or keywords
    >>> p[0] + p[1]                     # indexable like a plain tuple
    33
    >>> x, y = p                        # unpack like a regular tuple
    >>> x, y
    (11, 22)
    >>> p.x + p.y                       # fields also accessible by name
    33
    >>> d = p._asdict()                 # convert to a dictionary
    >>> d['x']
    11
    >>> Point(**d)                      # convert from a dictionary
    Point(x=11, y=22)
    >>> p._replace(x=100)               # _replace() is like str.replace() but targets named fields
    Point(x=100, y=22)

    """

    typename = _sys.intern(str("Outputs"))

    for name in [typename] + list(kwargs.keys()):
        if type(name) is not str:
            raise TypeError('Type names and field names must be strings')
        if not name.isidentifier():
            raise ValueError('Type names and field names must be valid '
                             f'identifiers: {name!r}')
        if _iskeyword(name):
            raise ValueError('Type names and field names cannot be a '
                             f'keyword: {name!r}')

    seen = set()
    for name in kwargs.keys():
        if name.startswith('_'):
            raise ValueError('Field names cannot start with an underscore: '
                             f'{name!r}')
        if name in seen:
            raise ValueError(f'Encountered duplicate field name: {name!r}')
        seen.add(name)

    # Variables used in the methods and docstrings
    field_names = tuple(map(_sys.intern, list(kwargs.keys())))
    num_fields = len(field_names)
    arg_list = repr(field_names).replace("'", "")[1:-1]
    repr_fmt = '(' + ', '.join(f'{name}=%r' for name in field_names) + ')'
    tuple_new = tuple.__new__
    _len = len

    # Create all the named tuple methods to be added to the class namespace

    s = f'def __new__(_cls, {arg_list}): return _tuple_new(_cls, ({arg_list}))'
    namespace = {'_tuple_new': tuple_new, '__name__': f'namedtuple_{typename}'}
    # Note: exec() has the side-effect of interning the field names
    exec(s, namespace)
    __new__ = namespace['__new__']
    __new__.__doc__ = f'Create new instance of {typename}({arg_list})'


    @classmethod
    def _make(cls, iterable):
        result = tuple_new(cls, iterable)
        if _len(result) != num_fields:
            raise TypeError(f'Expected {num_fields} arguments, got {len(result)}')
        return result

    _make.__func__.__doc__ = (f'Make a new {typename} object from a sequence '
                              'or iterable')

    def _replace(_self, **kwds):
        result = _self._make(map(kwds.pop, field_names, _self))
        if kwds:
            raise ValueError(f'Got unexpected field names: {list(kwds)!r}')
        return result

    _replace.__doc__ = (f'Return a new {typename} object replacing specified '
                        'fields with new values')

    def __repr__(self):
        'Return a nicely formatted representation string'
        return self.__class__.__name__ + repr_fmt % self

    def _asdict(self):
        'Return a new OrderedDict which maps field names to their values.'
        return OrderedDict(zip(self._fields, self))

    def __getnewargs__(self):
        'Return self as a plain tuple.  Used by copy and pickle.'
        return tuple(self)

    def _typed_fields(self) -> Dict[str, type]:
        return self.__typed_fields__

    # Modify function metadata to help with introspection and debugging

    for method in (__new__, _make.__func__, _replace,
                   __repr__, _asdict, __getnewargs__, 
                   _typed_fields):
        method.__qualname__ = f'{typename}.{method.__name__}'

    # Build-up the class namespace dictionary
    # and use type() to build the result class
    class_namespace = {
        '__doc__': f'{typename}({arg_list})',
        '__slots__': (),
        '_fields': field_names,
        '__new__': __new__,
        '_make': _make,
        '_replace': _replace,
        '__repr__': __repr__,
        '_asdict': _asdict,
        '__getnewargs__': __getnewargs__,
        '__typed_fields__': kwargs,
        'typed_outputs': _typed_fields
    }
    cache = {}
    for index, name in enumerate(field_names):
        try:
            itemgetter_object, doc = cache[index]
        except KeyError:
            itemgetter_object = _itemgetter(index)
            doc = f'Alias for field number {index}'
            cache[index] = itemgetter_object, doc
        class_namespace[name] = property(itemgetter_object, doc=doc)

    result = type(typename, (tuple, Outputs), class_namespace)

    # For pickling to work, the __module__ variable needs to be set to the frame
    # where the named tuple is created.  Bypass this step in environments where
    # sys._getframe is not defined (Jython for example) or sys._getframe is not
    # defined for arguments greater than 0 (IronPython), or where the user has
    # specified a particular module.
    try:
        module = _sys._getframe(1).f_globals.get('__name__', '__main__')
    except (AttributeError, ValueError):
        pass
    if module is not None:
        result.__module__ = module

    return result

# class Outputs():
#     def __new__(cls, name, this_bases, d):
#         for k, v in kwargs.items():
#             kwargs[k] = _interface_model.Variable(
#                 type_to_literal_type(v),
#                 ''
#             )  # TODO: Support descriptions
    
#         return collections.namedtuple('Outputs', {k: v for k, v in kwargs.items()})

#     @classmethod
#     def __prepare__(cls, name, this_bases):
#         return meta.__prepare__(name, bases)
