import logging

from operator import itemgetter
from itertools import groupby
from collections import namedtuple

from qvarnmr.func import run
from qvarnmr.utils import is_empty
from qvarnmr.exceptions import HandlerVersionError
from qvarnmr.handlers import get_handlers

logger = logging.getLogger(__name__)


RESOURCE_CHANGES = CREATED, UPDATED, DELETED = ('created', 'updated', 'deleted')

Notification = namedtuple('Notification', [
    'resource_type',
    'resource_change',
    'resource_id',
    'notification_id',
    'listener_id',
    'generated',  # True - means notification is not coming from Qvarn, but is generated, and there
                  # is no point to delete it.
])

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


def _save_map_results(qvarn, handler, resource, target_resource_type, source_resource_type,
                      results):
    resources_updated = 0

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
        resources_updated += 1

    return resources_updated


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
    resources_updated = 0
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
            resources_updated += _save_map_results(qvarn, handler, resource, target_resource_type,
                                                   source_resource_type, results)

    elif resource_change == DELETED:
        for target_resource_type, handler in handlers:
            resources = qvarn.search(target_resource_type, _mr_source_id=resource_id)
            for resource in qvarn.get_multiple(target_resource_type, resources):
                # Until reduce was not yet processed, we don't want to delete this resource. Because
                # reduce handlers need to know the key.
                # All resources marked for deletion will be cleaned up after each update cycle.
                resource['_mr_deleted'] = True
                qvarn.update(target_resource_type, resource['id'], resource)
                resources_updated += 1

    else:
        raise ValueError('Unknown resource change type: %r' % resource_change)

    return resources_updated


def _map_reduce_resources(context, resources, handler):
    resources = context.qvarn.get_multiple(context.source_resource_type, resources)
    for resource in resources:
        for value in run(handler, context, resource):
            yield value


def _iter_reduce_resource_ids(qvarn, config, source_resource_type, key):
    resources = qvarn.search(source_resource_type, _mr_key=key,
                             show=('_mr_source_type', '_mr_version', '_mr_deleted'))
    for resource in resources:
        if not resource['_mr_deleted']:
            map_handler = config[source_resource_type][resource['_mr_source_type']]
            if map_handler['version'] != resource['_mr_version']:
                raise HandlerVersionError(key)
            yield resource['id']


def process_reduce(qvarn, config, source_resource_type, key, handlers, resync=False):
    context = Context(qvarn, source_resource_type)
    for target_resource_type, handler in handlers:
        target_resource = qvarn.search_one(target_resource_type, _mr_key=key, default=None)

        if resync and target_resource and _same_version(handler['version'], [target_resource]):
            # If we are doning full resync, skip resources that are already resynced.
            continue

        # Get all resources by given key, force result to be an iterator,
        # because in future we should query resources iteratively in order to
        # avoid huge memory consumptions.
        resources = _iter_reduce_resource_ids(qvarn, config, source_resource_type, key)

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


class MapReduceEngine:
    EVENTS = (
        'map_handler_processed',
        'reduce_handler_processed',
    )

    def __init__(self, qvarn, config):
        self.qvarn = qvarn
        self.config = config
        self.mappers, self.reducers = get_handlers(config)
        self.callbacks = {event: [] for event in self.EVENTS}
        self.reduce_handler_sources = {
            source
            for target, handlers in config.items()
            for source, handler in handlers.items()
            if handler['type'] == 'reduce'
        }

    def _run_callbacks(self, event):
        for callback in self.callbacks[event]:
            callback()

    def _process_map_handlers(self, changes, resync=False):
        changes_processed = 0
        reduce_changes = []

        # Run through all changes, process map handlers immediately and collect changes that have reduce
        # handlers for processing in groups in the next step.
        for notification in changes:
            try:
                logger.debug("processing map handlers for %r", (
                    notification.resource_type, notification.resource_change, notification.resource_id,
                ))
                process_map(self.qvarn, notification.resource_type, notification.resource_change,
                            notification.resource_id, self.mappers[notification.resource_type],
                            resync)

            except Exception:
                logger.exception("error while processing map handlers for %r", (
                    notification.resource_type, notification.resource_change,
                    notification.resource_id,
                ))

                # Delete notifications even if handler raised exception. If there is an error in
                # handler, there is no point retrying it. Once handler will be fixed, all his target
                # resources will updated anyway.
                _delete_notification(self.qvarn, notification)

            else:
                should_reduce = (
                    notification.resource_type in self.reduce_handler_sources and

                    # We ignore all delete notifications, since we don't delete mapped resources, we
                    # mark then as deleted first (and that generates UPDATED notification) and only then
                    # mapped resources are deleted (cleaned) completely. And once they are deleted for
                    # real, we are no longer interested in them.
                    notification.resource_change != DELETED
                )
                if should_reduce:
                    resource = self.qvarn.search_one(notification.resource_type,
                                                     id=notification.resource_id,
                                                     show=('_mr_key',), default=None)
                    if resource is None:
                        # It is very unlikely that we are going to end up here, but lets be sure.
                        logger.error("resource %s of type %s was deleted.",
                                     notification.resource_id, notification.resource_type)
                        # If for some unknown reasons resource was deleted, then we can't get the key,
                        # and without key, we can't call reduce handler. If this warning ever happens
                        # situations should be investigated and fixed.
                        _delete_notification(self.qvarn, notification)
                        changes_processed += 1
                    else:
                        # Collect all changes that have reduce handlers and process them later, grouped
                        # by key. This will lower number of reduce handler calls.
                        reduce_changes.append(((notification.resource_type, resource['_mr_key']),
                                               notification))
                else:
                    _delete_notification(self.qvarn, notification)
                    changes_processed += 1

            self._run_callbacks('map_handler_processed')

        return changes_processed, reduce_changes

    def _process_reduce_handlers(self, changes, changes_processed):
        # Process all changes with reduce handlers in groups.
        changes = sorted(changes, key=itemgetter(0))
        for (source_resource_type, key), group in groupby(changes, key=itemgetter(0)):
            try:
                process_reduce(self.qvarn, self.config, source_resource_type, key,
                               self.reducers[source_resource_type], resync=False)

            except HandlerVersionError as e:
                # If we end up here, it means, that this key has inconsistent versions in mapped
                # resources. In that case we postpone notification by leaving undeleted.
                logger.debug("incompatible mapped resource versions for key=%r of %r resource.",
                             e.key, source_resource_type)

                # TODO: update all outdated reduce source resources, because otherwise reduce will have
                #       to wait until whole resync process is done.

            except Exception:
                logger.exception("error while processing reduce handlers for %r, key=%r",
                                 source_resource_type, key)

                # Delete notifications even if handler raised exception. If there is an error in
                # handler, there is no point retrying it. Once handler will be fixed, all his target
                # resources will updated anyway.
                for _, notification in group:
                    _delete_notification(self.qvarn, notification)

            else:
                # Delete processed mapped resources if they where marked for deletion.
                for resource_id in self.qvarn.search(source_resource_type, _mr_key=key,
                                                     _mr_deleted=True):
                    self.qvarn.delete(source_resource_type, resource_id)

                # Delete processed notifications.
                for _, notification in group:
                    _delete_notification(self.qvarn, notification)
                    changes_processed += 1

            self._run_callbacks('reduce_handler_processed')

        return changes_processed

    def add_callback(self, event, callback):
        self.callbacks[event].append(callback)

    def process_changes(self, changes, resync=False):
        changes_processed, reduce_changes = self._process_map_handlers(changes, resync)
        changes_processed = self._process_reduce_handlers(reduce_changes, changes_processed)
        return changes_processed


def get_changes(qvarn, listeners):
    for resource_type, listener, state in listeners:
        path = resource_type + '/listeners/' + listener['id'] + '/notifications'
        for notification_id in qvarn.get_list(path):
            notification = qvarn.get(path, notification_id)
            yield Notification(
                resource_type=resource_type,
                resource_change=notification['resource_change'],
                resource_id=notification['resource_id'],
                notification_id=notification['id'],
                listener_id=listener['id'],
                generated=False,
            )


def _delete_notification(qvarn, notification):
    if not notification.generated:
        logger.debug("delete notification %r", (
            notification.resource_type, notification.resource_change, notification.resource_id,
        ))

        path = notification.resource_type + '/listeners/' + notification.listener_id + '/notifications'
        qvarn.delete(path, notification.notification_id)
