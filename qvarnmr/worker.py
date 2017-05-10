from collections import defaultdict


class ContextVar:

    def __init__(self, name):
        assert name in ('qvarn', 'source_resource_type')
        self.name = name


def get_arg(context, value):
    if isinstance(value, ContextVar):
        return context[value.name]
    else:
        return value


def process_map(qvarn, source_resource_type, notification, resource, mappers):
    context = {
        'qvarn': qvarn,
        'source_resource_type': source_resource_type,
    }
    if notification['resource_change'] == 'created':
        for target_resource_type, mapper in mappers:
            args = [get_arg(context, a) for a in mapper['args']]
            for key, value in mapper['func'](resource, *args):
                value = value or {}
                value['_mr_key'] = key
                value['_mr_source_id'] = resource['id']
                value['_mr_source_type'] = source_resource_type
                value = qvarn.create(target_resource_type, value)
                yield target_resource_type, key, value
    else:
        raise ValueError('Unknown resource change type: %r' % notification['resource_change'])


def process_reduce(qvarn, source_resource_type, key, reducers):
    context = {
        'qvarn': qvarn,
        'source_resource_type': source_resource_type,
    }
    for target_resource_type, reducer in reducers:
        # Get all resources by given key
        resources = qvarn.search(source_resource_type, _mr_key=key)

        # Call reduce function for all resources matching key
        args = [get_arg(context, a) for a in reducer['args']]
        value = reducer['func'](resources, *args)

        # If reduce function returns non-dict value, store it to _mr_value
        if isinstance(value, dict):
            value['_mr_value'] = None
        else:
            value = {'_mr_value': value}

        # Also store _mr_key in order to be able to update this key with new
        # value, once one of the source resources will be changed.
        value['_mr_key'] = key

        # Save reduced value to the target resource type.
        resource = qvarn.search_one(target_resource_type, _mr_key=key, default=None)
        if resource is None:
            qvarn.create(target_resource_type, value)
        else:
            qvarn.update(target_resource_type, resource['id'], value)


def process(qvarn, listeners, config):
    mappers = defaultdict(list)
    reducers = defaultdict(list)
    for target_resource_type, handlers in config.items():
        for handler in handlers:
            if handler['type'] == 'map':
                mappers[handler['source']].append((target_resource_type, handler))
            elif handler['type'] == 'reduce':
                reducers[handler['source']].append((target_resource_type, handler))
            else:
                raise ValueError('Unknown map/reduce type, it should be map or reduce, got: %r' % handler['type'])

    for resource_type, listener in listeners:
        notifications = qvarn.get_list(resource_type + '/listeners/' + listener['id'] + '/notifications')
        for notification in notifications:
            notification = qvarn.get(resource_type + '/listeners/' + listener['id'] + '/notifications', notification)
            resource = qvarn.get(resource_type, notification['resource_id'])

            # Process map functions.
            for target_resource_type, key, value in process_map(qvarn, resource_type, notification, resource, mappers[resource_type]):
                # Process all touched keys with reduce functions.
                # That means, there is no need to listen notifications of map
                # target resource types.
                process_reduce(qvarn, target_resource_type, key, reducers[target_resource_type])

            # Consider this change to be done.
            qvarn.delete(resource_type + '/listeners/' + listener['id'] + '/notifications', notification)
