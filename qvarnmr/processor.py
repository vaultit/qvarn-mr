import time
import logging

from operator import itemgetter
from itertools import groupby
from collections import namedtuple

from qvarnmr.clients.qvarn import QvarnResourceNotFound
from qvarnmr.exceptions import HandlerVersionError
from qvarnmr.func import run
from qvarnmr.handlers import get_handlers
from qvarnmr.utils import is_empty

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

FailedNotification = namedtuple('FailedNotification', Notification._fields + (
    'retries',
    'processed_at',
))

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
    qvarn.delete_multiple(target_resource_type, [x['id'] for x in resources])


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

    # Timestamp in nanoseconds, Qvarn uses signed 8 bytes integer for storing numbers. So if we
    # store timestamp in nanoseconds, we have enough space until ~2270 year.
    value['_mr_timestamp'] = int(time.time() * 1e9)

    # Save reduced value to the target resource type.
    if resource is None:
        qvarn.create(target_resource_type, value)
    else:
        qvarn.update(target_resource_type, resource['id'], value)


def _process_map(qvarn, source_resource_type, resource_change, resource_id, handlers, resync=False):
    resources_updated = 0
    context = Context(qvarn, source_resource_type)
    if resource_change in (CREATED, UPDATED):
        resource = qvarn.get(source_resource_type, resource_id)
        for target_resource_type, handler in handlers:
            logger.info('processing map handler source=%s target=%s change=%s resource=%s '
                        'handler=%r version=%s resync=%r', source_resource_type,
                        target_resource_type, resource_change, resource_id, handler['handler'],
                        handler['version'], resync)
            start = time.time()

            existing_resources = qvarn.search(target_resource_type, _mr_source_id=resource['id'],
                                              show=('_mr_version',))

            if resync and _same_version(handler['version'], existing_resources):
                # If we are doning full resync, skip resources that are already resynced.
                continue

            # Run map handler.
            # If handler fails, nothing will be updated.
            results = list(run(handler['handler'], context, resource))

            # We have to clean all existing resources produced by map handler previously, because we
            # can't easily identify previously generated (key, value) pairs with the new ones.
            _clean_existing_resources(qvarn, target_resource_type, existing_resources)
            resources_updated += _save_map_results(qvarn, handler, resource, target_resource_type,
                                                   source_resource_type, results)
            logger.info('done processing map handler source=%s target=%s change=%s resource=%s '
                        'handler=%r version=%s resync=%r output=%d time=%.2fs',
                        source_resource_type, target_resource_type, resource_change, resource_id,
                        handler['handler'], handler['version'], resync, len(results),
                        time.time() - start)

    elif resource_change == DELETED:
        for target_resource_type, handler in handlers:
            logger.info('processing map handler source=%s target=%s change=%s resource=%s '
                        'handler=%r version=%s resync=%r', source_resource_type,
                        target_resource_type, resource_change, resource_id, handler['handler'],
                        handler['version'], resync)
            start = time.time()

            resources = qvarn.search(target_resource_type, _mr_source_id=resource_id)
            for resource in qvarn.get_multiple(target_resource_type, resources):
                # Until reduce was not yet processed, we don't want to delete this resource. Because
                # reduce handlers need to know the key.
                # All resources marked for deletion will be cleaned up after each update cycle.
                resource['_mr_deleted'] = True
                qvarn.update(target_resource_type, resource['id'], resource)
                resources_updated += 1

            logger.info('done processing map handler source=%s target=%s change=%s resource=%s '
                        'handler=%r version=%s resync=%r time=%.2fs', source_resource_type,
                        target_resource_type, resource_change, resource_id, handler['handler'],
                        handler['version'], resync, time.time() - start)

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
            # We need to wait, while all source resources have same version and only then do the
            # reduce part, when all data are consistent.
            if map_handler['version'] != resource['_mr_version']:
                raise HandlerVersionError(key)
            yield resource['id']


def _get_and_ensure_single_resource(qvarn, resource_type, key):
    resources = qvarn.search(resource_type, _mr_key=key, show_all=True)

    if len(resources) > 1:
        resources = sorted(resources, key=lambda x: (x['_mr_timestamp'] or 0), reverse=True)
        _clean_existing_resources(qvarn, resource_type, resources[1:])

    if len(resources) > 0:
        return resources[0]


def _process_reduce(qvarn, config, source_resource_type, key, handlers, resync=False):
    context = Context(qvarn, source_resource_type)
    for target_resource_type, handler in handlers:
        logger.info('processing reduce handler source=%s target=%s key=%s handler=%r '
                    'version=%s resync=%r', source_resource_type, target_resource_type, key,
                    handler['handler'], handler['version'], resync)
        start = time.time()

        target_resource = _get_and_ensure_single_resource(qvarn, target_resource_type, key)

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

        else:
            # Call reduce function for all resources matching key.
            value = next(run(handler['handler'], context, resources), None)
            _save_reduce_result(qvarn, handler, target_resource, target_resource_type, key, value)

            logger.info('done processing reduce handler source=%s target=%s key=%s handler=%r '
                        'version=%s resync=%r time=%.2fs', source_resource_type,
                        target_resource_type, key, handler['handler'], handler['version'], resync,
                        time.time() - start)


class MapReduceEngine:
    EVENTS = (
        'map_handler_processed',
        'reduce_handler_processed',
    )

    def __init__(self, qvarn, config, raise_errors=False):
        self.qvarn = qvarn
        self.config = config
        self.raise_errors = raise_errors
        self.mappers, self.reducers = get_handlers(config)
        self.callbacks = {event: [] for event in self.EVENTS}
        self.reduce_handler_sources = {
            source
            for target, sources in config.items()
            for source, handler in sources.items()
            if handler['type'] == 'reduce'
        }

        self._failed_notifications = {}

    def _run_callbacks(self, event):
        for callback in self.callbacks[event]:
            callback()

    def _report_success(self, notifications):
        for notification in notifications:
            if notification.notification_id in self._failed_notifications:
                del self._failed_notifications[notification.notification_id]
            _delete_notification(self.qvarn, notification)

    def _report_error(self, notifications):
        for notification in notifications:
            key = notification.notification_id
            error = self._failed_notifications.get(key, None)
            if error is None:
                self._failed_notifications[key] = FailedNotification(**dict(
                    notification._asdict(),
                    retries=0,
                    processed_at=time.time(),
                ))
            else:
                if error.retries > 1:
                    del self._failed_notifications[key]
                    _delete_notification(self.qvarn, notification)
                else:
                    self._failed_notifications[key] = FailedNotification(**dict(
                        notification._asdict(),
                        retries=error.retries + 1,
                        processed_at=error.processed_at,
                    ))

    def _iter_changes(self, changes):
        for notification in changes:

            # Retry failed notifications.
            if notification.notification_id in self._failed_notifications:
                now = time.time()
                notification = self._failed_notifications[notification.notification_id]
                if notification.retries == 0 and now - notification.processed_at < 0.25:
                    logger.debug('retry 0.25 (skip): %s', now - notification.processed_at)
                    continue
                elif notification.retries == 1 and now - notification.processed_at < 1.5:
                    logger.debug('retry 1.5 (skip): %s', now - notification.processed_at)
                    continue
                elif notification.retries > 1:
                    logger.debug('retry > 1 (abort)')
                    del self._failed_notifications[notification.notification_id]
                    _delete_notification(self.qvarn, notification)
                    continue
                logger.debug("retrying failed notification, resource: %s id: %s, retry: %s "
                             "delay: %s", notification.resource_type, notification.resource_id,
                             notification.retries, now - notification.processed_at)

            yield notification

    def _process_map_handlers(self, changes, resync=False):
        changes_processed = 0
        errors = 0
        reduce_changes = []

        # Run through all changes, process map handlers immediately and collect changes that have
        # reduce handlers for processing in groups in the next step.
        for notification in changes:
            try:
                handlers = self.mappers[notification.resource_type]
                if handlers:
                    _process_map(
                        self.qvarn, notification.resource_type, notification.resource_change,
                        notification.resource_id, handlers, resync,
                    )

            except Exception:
                # XXX: probably errors should be handler inside _process_map and another
                #      exception could be rerised with information about which handler failed.
                logger.exception("error while processing map handlers for %r", (
                    notification.resource_type, notification.resource_change,
                    notification.resource_id,
                ))
                self._report_error([notification])
                errors += 1
                if self.raise_errors:
                    raise

            else:
                should_reduce = (
                    notification.resource_type in self.reduce_handler_sources and

                    # We ignore all delete notifications, since we don't delete mapped resources, we
                    # mark then as deleted first (and that generates UPDATED notification) and only
                    # then mapped resources are deleted (cleaned) completely. And once they are
                    # deleted for real, we are no longer interested in them.
                    notification.resource_change != DELETED
                )
                if should_reduce:
                    resource = self.qvarn.search_one(notification.resource_type,
                                                     id=notification.resource_id,
                                                     show=('_mr_key',), default=None)
                    if resource is None:
                        logger.warning(
                            "can't find resource (%s, %s) specifiend in notificaton, the resource "
                            "could be deleted or not yet replicated", notification.resource_type,
                            notification.resource_id)
                        self._report_error([notification])
                        errors += 1
                    else:
                        # Collect all changes that have reduce handlers and process them later,
                        # grouped by key. This will lower number of reduce handler calls.
                        reduce_changes.append(((notification.resource_type, resource['_mr_key']),
                                               notification))
                else:
                    self._report_success([notification])
                    changes_processed += 1

            self._run_callbacks('map_handler_processed')

        return changes_processed, errors, reduce_changes

    def process_reduce_handlers(self, changes, *, errors=0, resync=False):
        changes_processed = 0

        # Process all changes with reduce handlers in groups.
        changes = sorted(changes, key=itemgetter(0))
        for (source_resource_type, key), group in groupby(changes, key=itemgetter(0)):
            try:
                _process_reduce(self.qvarn, self.config, source_resource_type, key,
                                self.reducers[source_resource_type], resync=resync)

            except HandlerVersionError as e:
                # If we end up here, it means, that this key has inconsistent versions in mapped
                # resources. In that case we postpone notification by leaving undeleted.
                logger.debug("incompatible mapped resource versions for key=%r of %r resource.",
                             e.key, source_resource_type)
                self._report_error([notification for _, notification in group])

            except Exception:
                # XXX: probably errors should be handler inside _process_reduce and another
                #      exception could be rerised with information about which handler failed.
                logger.exception("error while processing reduce handlers for %r, key=%r",
                                 source_resource_type, key)
                notifications = [notification for _, notification in group]
                self._report_error(notifications)
                errors += len(notifications)
                if self.raise_errors:
                    raise

            else:
                # Delete processed mapped resources if they where marked for deletion.
                for resource_id in self.qvarn.search(source_resource_type, _mr_key=key,
                                                     _mr_deleted=True):
                    self.qvarn.delete(source_resource_type, resource_id)

                notifications = [notification for _, notification in group]
                self._report_success(notifications)
                changes_processed += len(notifications)

            self._run_callbacks('reduce_handler_processed')

        return changes_processed, errors

    def add_callback(self, event, callback):
        self.callbacks[event].append(callback)

    def process_changes(self, changes, resync=False):
        logger.info('processing changes resync=%r', resync)
        start = time.time()
        changes = self._iter_changes(changes)
        mapped, errors, reduce_changes = self._process_map_handlers(changes, resync)
        reduced, errors = self.process_reduce_handlers(reduce_changes, errors=errors, resync=resync)
        logger.info('done processing changes resync=%r mapped=%d reduced=%d errors=%d '
                    'time=%.2fs', resync, mapped, reduced, errors, time.time() - start)
        return mapped + reduced


def get_changes(qvarn, listeners):
    l = list(listeners)  # create a new copy of listeners
    for resource_type, listener, state in l:
        path = resource_type + '/listeners/' + listener['id'] + '/notifications'
        for notification_id in qvarn.get_list(path):
            try:
                notification = qvarn.get(path, notification_id)
            except QvarnResourceNotFound:
                logger.warning("master:   notification has been deleted (probably after giving up "
                               "retries): notification=%s resource_type=%s", notification_id,
                               resource_type)
                # continue loop withouth yielding
                continue
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
        logger.debug("delete notification for resource type=%s change=%s resource=%s",
                     notification.resource_type, notification.resource_change,
                     notification.resource_id)

        path = notification.resource_type + '/listeners/' + notification.listener_id + '/notifications'
        qvarn.delete(path, notification.notification_id)
