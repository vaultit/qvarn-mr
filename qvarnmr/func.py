from functools import wraps


class Func:

    def __init__(self, func, *args, **kwargs):
        self.func = func
        self.args = args
        self.kwargs = kwargs

    def __repr__(self):
        return '%r wrapped by %s' % (self.func, super().__repr__())

    def __call__(self, context, value):
        return self.func(context, value, *self.args, **self.kwargs)


def mrfunc():
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            return Func(func, *args, **kwargs)
        return wrapper
    return decorator


def run(func, context, value):
    if isinstance(func, Func):
        return func(context, value)
    else:
        return func(value)


@mrfunc()
def items(context, resource, key, value=None):
    if value is None:
        yield resource[key], None
    else:
        yield resource[key], resource[value]


@mrfunc()
def values(context, resources, key='_mr_value'):
    for resource in context.qvarn.get_multiple(context.source_resource_type, resources):
        yield resource[key]


@mrfunc()
def join(context, resources, mapping):
    result = {}
    for resource in context.qvarn.get_multiple(context.source_resource_type, resources):
        source = context.qvarn.get(resource['_mr_source_type'], resource['_mr_source_id'])
        for key, name in mapping.get(source['type'], {}).items():
            name = name or key
            result[name] = source[key]
    return result
