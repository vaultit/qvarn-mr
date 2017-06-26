from itertools import chain
from types import GeneratorType

from qvarnmr.func import Func


def get_handler_identifier(handler):
    if isinstance(handler, Func):
        args = (
            [repr(x) for x in handler.args] +
            [k + '=' + repr(v) for k, v in handler.kwargs.items()]
        )
        return (
            handler.func.__module__ + '.' + handler.func.__name__ +
            '(' + ', '.join(args) + ')'
        )
    else:
        return handler.__module__ + '.' + handler.__name__


def chunks(size, items):
    if not isinstance(items, GeneratorType):
        items = iter(items)
    while True:
        yield chain(
            [next(items)],
            (next(items) for i in range(size - 1)),
        )


def is_empty(items):
    try:
        item = next(items)
    except StopIteration:
        return iter([]), True
    else:
        return chain([item], items), False
