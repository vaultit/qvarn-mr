import logging

from collections import namedtuple

from qvarnmr.func import run
from qvarnmr.handlers import get_handlers
from qvarnmr.utils import is_empty

logger = logging.getLogger(__name__)


RESOURCE_CHANGES = CREATED, UPDATED, DELETED = ('created', 'updated', 'deleted')


Context = namedtuple('Context', [
    'qvarn',
    'source_resource_type',
])


def _same_version(version, resources):
    existing_versions = {x['_mr_version'] for x in resources}
    if len(resources) == 1 and version in existing_versions:
        return True
    else:
        return False


def _clean_existing_resources(qvarn, target_resource_type, resources):
    for resource in resources:
        qvarn.delete(target_resource_type, resource['id'])


def _clean_deleted_resources(qvarn, target_resource_types):
    for resource_type in target_resource_types:
        for resource_id in qvarn.search(resource_type, _mr_deleted=True):
            qvarn.delete(resource_type, resource_id)


def _save_map_results(qvarn, handler, resource, target_resource_type, source_resource_type,
                      results):

    for key, value in results:
        if isinstance(value, dict):
            value['_mr_value'] = None
        else:
            value = {'_mr_value': value}

        value['_mr_key'] = key
        value['_mr_source_id'] = resource['id']
        value['_mr_source_type'] = source_resource_type
        value['_mr_deleted'] = False
        value['_mr_version'] = handler['version']

        qvarn.create(target_resource_type, value)

        yield target_resource_type, key


def _save_reduce_result(qvarn, handler, resource, target_resource_type, key, value):
    # If reduce function returns non-dict value, store it to _mr_value.
    if isinstance(value, dict):
        value['_mr_value'] = None
    else:
        value = {'_mr_value': value}

    # Also store _mr_key in order to be able to update this key with new
    # value, once one of the source resources will be changed.
    value['_mr_key'] = key

    # Save handler version to be able to track outdated resources.
    value['_mr_version'] = handler['version']

    # Save reduced value to the target resource type.
    if resource is None:
        qvarn.create(target_resource_type, value)
    else:
        qvarn.update(target_resource_type, resource['id'], value)


def process_map(qvarn, source_resource_type, resource_change, resource_id, handlers, resync=False):
    context = Context(qvarn, source_resource_type)
    if resource_change in (CREATED, UPDATED):
        resource = qvarn.get(source_resource_type, resource_id)
        for target_resource_type, handler in handlers:
            existing_resources = qvarn.search(target_resource_type, _mr_source_id=resource['id'],
                                              show=('_mr_version',))

            if resync and _same_version(handler['version'], existing_resources):
                # If we are doning full resync, skip resources that are already resynced.
                continue

            # Run map handler.
            # If handler fails, nothing will be updated.
            results = list(run(handler['handler'], context, resource))

            # We have to clean all existing resources produced by map handler previously, because we
            # can't easily identify previously generated (key, value) pair with the new one.
            _clean_existing_resources(qvarn, target_resource_type, existing_resources)
            yield from _save_map_results(qvarn, handler, resource, target_resource_type,
                                         source_resource_type, results)

    elif resource_change == DELETED:
        for target_resource_type, handler in handlers:
            for resource in qvarn.search(target_resource_type, _mr_source_id=resource_id,
                                         show=('revision', '_mr_key')):
                # Until reduce was not yet process, we don't want to delete this resource. Because
                # reduce handlers need to know the key.
                # All resources marked for deletion will be cleaned up after each update cycle.
                qvarn.update(target_resource_type, resource['id'], {
                    'revision': resource['revision'],
                    '_mr_deleted': True,
                })
                yield target_resource_type, resource['_mr_key']

    else:
        raise ValueError('Unknown resource change type: %r' % resource_change)


def _map_reduce_resources(context, resources, handler):
    resources = context.qvarn.get_multiple(context.source_resource_type, resources)
    for resource in resources:
        for value in run(handler, context, resource):
            yield value


def process_reduce(qvarn, source_resource_type, key, handlers, resync=False):
    context = Context(qvarn, source_resource_type)
    for target_resource_type, handler in handlers:
        target_resource = qvarn.search_one(target_resource_type, _mr_key=key, default=None)

        if resync and target_resource and _same_version(handler['version'], [target_resource]):
            # If we are doning full resync, skip resources that are already resynced.
            continue

        # Get all resources by given key, force result to be an iterator,
        # because in future we should query resources iteratively in order to
        # avoid huge memory consumptions.
        resources = iter(qvarn.search(source_resource_type, _mr_key=key))

        if 'map' in handler:
            resources = _map_reduce_resources(context, resources, handler['map'])

        resources, empty = is_empty(resources)
        if target_resource and empty:
            # Delete key entry if there are no keys produced by map handlers.
            _clean_existing_resources(qvarn, target_resource_type, [target_resource])
            continue

        # Call reduce function for all resources matching key.
        value = next(run(handler['handler'], context, resources), None)
        _save_reduce_result(qvarn, handler, target_resource, target_resource_type, key, value)


def process_changes(qvarn, changes, mappers, reducers, resync=False):
    changes_processed = 0
    map_target_resources_types = set()

    for resource_type, resource_change, resource_id, done in changes:
        try:
            logger.debug("Processing map/reduce handlers for %r",
                         (resource_type, resource_change, resource_id))

            # Process all changed resources with map handlers.
            keys = set(process_map(qvarn, resource_type, resource_change, resource_id,
                                   mappers[resource_type], resync))
            changes_processed += len(keys)

            # Process all touched keys with reduce handlers.
            for source_resource_type, key in keys:
                process_reduce(qvarn, source_resource_type, key, reducers[source_resource_type],
                               resync=False)

            # Update set of map target resources types to be cleaned.
            map_target_resources_types.update(resource_type for resource_type, key in keys)

        except Exception:
            logger.exception("Error while processing map/reduce handlers for %r",
                             (resource_type, resource_change, resource_id))
        else:
            done()

    # Clean map target resources marked for deletion after all changes has been processed.
    _clean_deleted_resources(qvarn, map_target_resources_types)

    return changes_processed


def process(qvarn, listeners, config):
    mappers, reducers = get_handlers(config)
    changes = get_changes(qvarn, listeners)
    process_changes(qvarn, changes, mappers, reducers)


def get_changes(qvarn, listeners):
    for resource_type, listener in listeners:
        path = resource_type + '/listeners/' + listener['id'] + '/notifications'
        notifications = qvarn.get_list(path)
        for notification in notifications:
            notification = qvarn.get(path, notification)
            yield (
                resource_type,
                notification['resource_change'],
                notification['resource_id'],
                lambda: qvarn.delete(path, notification['id'])
            )
