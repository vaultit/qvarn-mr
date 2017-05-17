from collections import namedtuple

from qvarnmr.func import run
from qvarnmr.handlers import get_handlers


RESOURCE_CHANGES = CREATED, UPDATED, DELETED = ('created', 'updated', 'deleted')


Context = namedtuple('Context', [
    'qvarn',
    'source_resource_type',
])


def process_map(qvarn, source_resource_type, resource_change, resource_id, mappers):
    context = Context(qvarn, source_resource_type)
    if resource_change in (CREATED, UPDATED):
        resource = qvarn.get(source_resource_type, resource_id)
        for target_resource_type, mapper in mappers:
            for key, value in run(mapper['map'], context, resource):
                if isinstance(value, dict):
                    value['_mr_value'] = None
                else:
                    value = {'_mr_value': value}

                value['_mr_key'] = key
                value['_mr_source_id'] = resource['id']
                value['_mr_source_type'] = source_resource_type
                target_resource = qvarn.search_one(target_resource_type, _mr_source_id=resource['id'], default=None)
                if target_resource:
                    value['id'] = target_resource['id']
                    value['revision'] = target_resource['revision']
                    value = qvarn.update(target_resource_type, target_resource['id'], value)
                else:
                    value = qvarn.create(target_resource_type, value)
                yield target_resource_type, key

    elif resource_change == DELETED:
        for target_resource_type, mapper in mappers:
            target_resource = qvarn.search_one(target_resource_type, _mr_source_id=resource_id, default=None)
            if target_resource:
                yield target_resource_type, target_resource['_mr_key']
                qvarn.delete(target_resource_type, target_resource['id'])

    else:
        raise ValueError('Unknown resource change type: %r' % resource_change)


def process_reduce(qvarn, source_resource_type, key, reducers):
    context = Context(qvarn, source_resource_type)
    for target_resource_type, reducer in reducers:
        # Get all resources by given key, force result to be an iterator,
        # because in future we should query resources iteratively in order to
        # avoid huge memory consumptions.
        resources = iter(qvarn.search(source_resource_type, _mr_key=key))

        if 'map' in reducer:
            resources = (
                x
                for resource in qvarn.get_multiple(source_resource_type, resources)
                for x in run(reducer['map'], context, resource)
            )

        # Call reduce function for all resources matching key
        value = run(reducer['reduce'], context, resources)

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


def get_changes(qvarn, listeners):
    for resource_type, listener in listeners:
        notifications = qvarn.get_list(resource_type + '/listeners/' + listener['id'] + '/notifications')
        for notification in notifications:
            notification = qvarn.get(resource_type + '/listeners/' + listener['id'] + '/notifications', notification)
            yield resource_type, notification['resource_change'], notification['resource_id']

            # Consider this change to be done.
            qvarn.delete(resource_type + '/listeners/' + listener['id'] + '/notifications', notification['id'])


def process_changes(qvarn, changes, mappers, reducers):
    changes_processed = 0

    for resource_type, resource_change, resource_id in changes:
        # Process all changed resources with map handlers.
        keys = set(process_map(qvarn, resource_type, resource_change, resource_id, mappers[resource_type]))
        changes_processed += len(keys)

        # Process all touched keys with reduce handlers.
        for target_resource_type, key in keys:
            process_reduce(qvarn, target_resource_type, key, reducers[target_resource_type])

    return changes_processed


def process(qvarn, listeners, config):
    mappers, reducers = get_handlers(config)
    changes = get_changes(qvarn, listeners)
    process_changes(qvarn, changes, mappers, reducers)
