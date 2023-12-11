__all__ = ('DeferredOperand', 'DeferredOperandConstructorValueMeta')

class DeferredOperand:
    """
    Returns an anonymous function that will mirror whatever operation is performed on this object.

    Can also be used with a callback to provide more control. Useful for building syntactic sugar.

    Calling and property/member access are supported and return new DeferredOperand objects.

    Example usage:

    >>> is_six = DeferredOperand() == 6
    >>> is_six(7)
    False
    >>> is_six(6)
    True
    >>> from functools import partial
    >>> is_six = DeferredOperand(lambda test: partial(filter, test)) == 6
    >>> list(is_six([6, 1, 2, 3, 4, 5, 6]))
    [6, 6]
    >>> happiness = {'cookies':2, 'pie':1, 'ice cream': 3}
    >>> has_cookies = DeferredOperand()['cookies'] > 0
    >>> has_cookies(happiness)
    True
    >>> sadness = {'brussels sprouts': 999999999, 'sauerkraut': 11111}
    >>> has_cookies_safe = DeferredOperand().get('cookies', 0) > 0
    >>> has_cookies_safe(sadness)
    False
    """
    def __init__(self, callback=None, prev=None):
        self._callback = callback
        self._prev = prev
        self._action = None
    
    def __getitem__(self, name):
        return self._deferred_chain(lambda val: val[name])
    
    def __getattr__(self, name):
        return self._deferred_chain(lambda val: getattr(val, name))

    def __call__(self, *args, **kwargs):
        return self._deferred_chain(lambda val: val(*args, **kwargs))
    
    @property
    def nullsafe(self):
        """
        Make this deferred operation null-safe.
        
        If the value is None, return None instead of doing the operation. Otherwise, proceed 
        normally. This is most useful chained with call and method/property access.

        >>> type_is_pizza = DeferredOperation().nullsafe['type'] == 'pizza'
        >>> type_is_pizza({'type':'hamburger'})
        False
        >>> type_is_pizza({'type':'pizza'})
        True
        >>> type_is_pizza(None)
        None
        """
        # FIXME failing in test_chaining
        return DeferredOperand(lambda operation: lambda val: None if val is None else operation(val), self)

    def _chain_actions(self, operation):
        if self._prev is not None and self._prev._action is not None:
            self._action = lambda val: operation(self._prev._action(val))
        else:
            self._action = operation
        return self._action
        
    
    def _apply_callback(self, operation):
        """
        Internal method used to work with the optional callback argument.
        """
        operation = self._chain_actions(operation)
        if self._callback is None:
            return operation
        else:
            return self._callback(operation)
    
    def _deferred_chain(self, operation):
        """
        Internal method used to deal with chaining operations, such as getattr and setattr
        """
        operation = self._chain_actions(operation)
        return DeferredOperand(self._callback, self)

    
    @property
    def return_(self):
        """
        A deferred operation to return the value directly.
        
        Most useful when chained with a call or member/property access:

        >>> get_stuff = DeferredOperand()['stuff'].return_
        >>> get_stuff({'stuff':things'})
        'things'
        """
        return self._apply_callback(lambda val: val)
    
    def __lt__(self, other):
        return self._apply_callback(lambda val: val < other)

    def __le__(self, other):
        return self._apply_callback(lambda val: val <= other)

    def __eq__(self, other):
        return self._apply_callback(lambda val: val == other)

    def __ne__(self, other):
        return self._apply_callback(lambda val: val != other)

    def __gt__(self, other):
        return self._apply_callback(lambda val: val > other)

    def __ge__(self, other):
        return self._apply_callback(lambda val: val >= other)
    
    def __contains__(self, other):
        return self._apply_callback(lambda val: other in val)
    
    def in_(self, *other):
        """
        Support the ``in`` operator, since it can't be used directly. Pass a single iterable, or multiple values.

        >>> is_abc = DeferredOperand().is_in(['a', 'b', 'c'])
        >>> is_abc('q')
        False
        >>> is_abc = DeferredOperand().is_in('a', 'b', 'c')
        >>> is_abc('b')
        True
        """
        if len(other) == 1:
            other = other[0]
        return self._apply_callback(lambda val: val in other)
    
    def not_in_(self, *other):
        """
        Support the ``not in`` operator, since it can't be used directly. Pass a single iterable, or multiple values.

        >>> not_abc = DeferredOperand().not_in(['a', 'b', 'c'])
        >>> not_abc('q')
        True
        >>> not_abc = DeferredOperand().not_in('a', 'b', 'c')
        >>> not_abc('b')
        False
        """
        if len(other) == 1:
            other = other[0]
        return self._apply_callback(lambda val: val not in other)
    
    def is_(self, other):
        """
        Support the ``is`` operator, since it can't be used directly.

        >>> is_none = DeferredOperand().is_(None)
        >>> is_none(None)
        True
        """
        return self._apply_callback(lambda val: val is other)
    
    def is_not_(self, other):
        """
        Support the ``is not`` operator, since it can't be used directly.

        >>> is_not_none = DeferredOperand().is_not_(None)
        >>> is_not_none(None)
        False
        """
        return self._apply_callback(lambda val: val is not other)

    def __add__(self, other):
        return self._apply_callback(lambda val: val + other)

    def __sub__(self, other):
        return self._apply_callback(lambda val: val - other)

    def __mul__(self, other):
        return self._apply_callback(lambda val: val * other)

    def __pow__(self, other, modulo=None):
        return self._apply_callback(lambda val: pow(val, other, modulo))

    def __truediv__(self, other):
        return self._apply_callback(lambda val: val / other)

    def __floordiv__(self, other):
        return self._apply_callback(lambda val: val // other)

    def __mod__(self, other):
        return self._apply_callback(lambda val: val % other)
    
    def __divmod__(self, other):
        return self._apply_callback(lambda val: divmod(val, other))

    def __lshift__(self, other):
        return self._apply_callback(lambda val: val << other)

    def __rshift__(self, other):
        return self._apply_callback(lambda val: val >> other)

    def __and__(self, other):
        return self._apply_callback(lambda val: val & other)

    def __or__(self, other):
        return self._apply_callback(lambda val: val | other)

    def __xor__(self, other):
        return self._apply_callback(lambda val: val ^ other)

    def __invert__(self):
        return self._apply_callback(lambda val: ~val)
    
    def __radd__(self, other):
        return self._apply_callback(lambda val: other + val)

    def __rsub__(self, other):
        return self._apply_callback(lambda val: other - val)

    def __rmul__(self, other):
        return self._apply_callback(lambda val: other * val)

    def __rtruediv__(self, other):
        return self._apply_callback(lambda val: other / val)

    def __rfloordiv__(self, other):
        return self._apply_callback(lambda val: other // val)

    def __rmod__(self, other):
        return self._apply_callback(lambda val: other % val)

    def __rdivmod__(self, other):
        return self._apply_callback(lambda val: divmod(other, val))

    def __rpow__(self, other, modulo=None):
        return self._apply_callback(lambda val: pow(other, val, modulo))

    def __rlshift__(self, other):
        return self._apply_callback(lambda val: other << val)

    def __rrshift__(self, other):
        return self._apply_callback(lambda val: other >> val)

    def __rand__(self, other):
        return self._apply_callback(lambda val: other & val)

    def __rxor__(self, other):
        return self._apply_callback(lambda val: other ^ val)

    def __ror__(self, other):
        return self._apply_callback(lambda val: other | val)
    
    def __neg__(self):
        return self._apply_callback(lambda val: -val)
    
    def __pos__(self):
        return self._apply_callback(lambda val: +val)
    
    def __abs__(self):
        return self._apply_callback(abs)

class DeferredOperandConstructorValueMeta(type):
    """
    Metaclass that adds a special `value` class property which calls the constructor with a 
    deferred operation.

    Example::

        class Mapper(metaclass=DeferredOperandConstructorValueMeta):
            def __init__(self, func):
                self.func = func
            def __call__(self, iterable):
                for value in iterable:
                     yield self.func(value) 

        # This...
        adds_5 = Mapper.value + 5
        # ...is equivalent to this...
        adds_5 = Mapper(lambda v: v + 5)
        # ...and yields this result:
        list(adds_5([1, 2, 3, 4, 5]))
        # [6, 7, 8, 9, 10]
    """
    @property
    def value(self):
        """
        Alternate constructor using `DeferredOperand`.

        Example::

            # This...
            (TheClass.value > 5) # Parens may be required due to operator precedence
            # ...is equivalent to this
            TheClass(lambda value: value > 5)
        """
        return DeferredOperand(self)