from types import GeneratorType
from functools import wraps


class Func:

    def __init__(self, func, *args, **kwargs):
        self.func = func
        self.args = args
        self.kwargs = kwargs

    def __repr__(self):
        return (
            self.func.__name__ + '(' +
            ', '.join(
                [repr(a) for a in self.args] +
                ['%s=%r' % (k, v) for k, v in self.kwargs.items()]
            ) + ')'
        )

    def __call__(self, context, value):
        return self.func(context, value, *self.args, **self.kwargs)


def mr_func():
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            return Func(func, *args, **kwargs)
        return wrapper
    return decorator


def run(func, context, value):
    if isinstance(func, Func):
        result = func(context, value)
    else:
        result = func(value)

    if isinstance(result, GeneratorType):
        yield from result
    else:
        yield result


def count(items):
    return sum(1 for x in items)


@mr_func()
def item(context, resource, key, value=None):
    if value is None:
        return resource[key], None
    else:
        return resource[key], resource[value]


@mr_func()
def value(context, resource, key='_mr_value'):
    return resource[key]


@mr_func()
def join(context, resources, mapping):
    result = {}
    for resource in context.qvarn.get_multiple(context.source_resource_type, resources):
        source = context.qvarn.get(resource['_mr_source_type'], resource['_mr_source_id'])
        for key, name in mapping.get(source['type'], {}).items():
            name = name or key
            result[name] = source[key]
    return result
