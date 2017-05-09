from collections import defaultdict


class ContextVar:

    def __init__(self, name):
        assert name in ('qvarn', 'resource_type')
        self.name = name


def get_arg(context, value):
    if isinstance(value, ContextVar):
        return context[value.name]
    else:
        return value


def process_reduce(qvarn, source, key, reducers):
    context = {
        'qvarn': qvarn,
        'resource_type': source,
    }
    for target, reducer in reducers:
        resources = qvarn.search(source, _mr_key=key)
        args = [get_arg(context, a) for a in reducer['args']]
        value = reducer['func'](resources, *args)
        if isinstance(value, dict):
            value['_mr_value'] = None
        else:
            value = {'_mr_value': value}
        value['_mr_key'] = key
        resource = qvarn.search_one(target, _mr_key=key, default=None)
        if resource is None:
            qvarn.create(target, value)
        else:
            qvarn.update(target, resource, resource['id'], value)


def process_map(qvarn, resource_type, notification, mappers):
    context = {
        'qvarn': qvarn,
        'resource_type': resource_type,
    }
    if notification['resource_change'] == 'created':
        resource = qvarn.get(resource_type, notification['resource_id'])
        for target, mapper in mappers:
            args = [get_arg(context, a) for a in mapper['args']]
            for key, value in mapper['func'](resource, *args):
                value = value or {}
                value['_mr_key'] = key
                value['_mr_source_id'] = resource['id']
                value['_mr_source_type'] = resource_type
                value = qvarn.create(target, value)
                yield target, key, value
    else:
        raise ValueError('Unknown resource change type: %r' % notification['resource_change'])


def process(qvarn, listeners, config):
    mappers = defaultdict(list)
    reducers = defaultdict(list)
    for target, handlers in config.items():
        for handler in handlers:
            if handler['type'] == 'map':
                mappers[handler['source']].append((target, handler))
            elif handler['type'] == 'reduce':
                reducers[handler['source']].append((target, handler))
            else:
                raise ValueError('Unknown map/reduce type, it should be map or reduce, got: %r' % handler['type'])

    for resource_type, listener in listeners:
        notifications = qvarn.get_list(resource_type + '/listeners/' + listener['id'] + '/notifications')
        for notification in notifications:
            notification = qvarn.get(resource_type + '/listeners/' + listener['id'] + '/notifications', notification)
            for target, key, value in process_map(qvarn, resource_type, notification, mappers[resource_type]):
                process_reduce(qvarn, target, key, reducers[target])
            qvarn.delete(resource_type + '/listeners/' + listener['id'] + '/notifications', notification)
