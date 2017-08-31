import time
import logging

from qvarnmr.clients.qvarn import QvarnApi
from qvarnmr.processor import UPDATED, Notification, MapReduceEngine, process_reduce
from qvarnmr.utils import chunks

logger = logging.getLogger(__name__)


def _no_action():
    pass


def iter_map_resync_changes(qvarn: QvarnApi, source_resource_type: str):
    for resource_id in qvarn.get_list(source_resource_type):
        yield Notification(
            resource_type=source_resource_type,
            resource_change=UPDATED,
            resource_id=resource_id,
            notification_id=None,
            listener_id=None,
            generated=True,
        )


def iter_reduce_resync_keys(qvarn: QvarnApi, source_resource_type: str):
    reduced_keys = set()
    for resource in qvarn.search(source_resource_type, show=('_mr_key',)):
        key = resource['_mr_key']
        if key not in reduced_keys:
            yield key
            reduced_keys.add(key)


def update_handler_version(qvarn, instance, target_resource_type, source_resource_type, version):
    state = qvarn.search_one(
        'qvarnmr_handlers',
        target=target_resource_type,
        source=source_resource_type,
        default=None,
    )

    if state is None:
        qvarn.create('qvarnmr_handlers', {
            'instance': instance,
            'target': target_resource_type,
            'source': source_resource_type,
            'version': version,
        })
    else:
        qvarn.update('qvarnmr_handlers', state['id'], {
            'revision': state['revision'],
            'instance': instance,
            'target': target_resource_type,
            'source': source_resource_type,
            'version': version,
        })


def iter_changed_handlers(qvarn: QvarnApi, config: dict, handler_type: str):
    for target_resource_type, handlers in config.items():
        for source_resource_type, handler in handlers.items():
            if handler['type'] == handler_type:
                state = qvarn.search_one(
                    'qvarnmr_handlers',
                    target=target_resource_type,
                    source=source_resource_type,
                    default=None,
                )
                if state is None or state['version'] != handler['version']:
                    yield target_resource_type, source_resource_type, handler


def resync_changed_handlers(qvarn: QvarnApi, engine: MapReduceEngine, instance: str):
    # First resync all map handlers.
    handlers = iter_changed_handlers(qvarn, engine.config, 'map')
    for target_resource_type, source_resource_type, handler in handlers:
        logger.info("full map resync source=%s target=%s handler=%r version=%s",
                    source_resource_type, target_resource_type, handler['handler'],
                    handler['version'])
        start = time.time()

        for changes in chunks(100, iter_map_resync_changes(qvarn, source_resource_type)):
            engine.process_changes(changes, resync=True)
            yield
        # Update handler version only when full resync is successfully done.
        update_handler_version(qvarn, instance, target_resource_type, source_resource_type,
                               handler['version'])
        logger.info("done full map resync source=%s target=%s handler=%r version=%s time=%.2fs",
                    source_resource_type, target_resource_type, handler['handler'],
                    handler['version'], time.time() - start)

    # Resync reduce handlers separately, because in order to resync reduce handlers, we don't need
    # to resync map handlers. And by the way, `process_changes` automatically calls
    # `process_reduce`, so here we might have some duplication if both, map and related reduce
    # handlers where updated. That is something, that could be optimized.
    handlers = iter_changed_handlers(qvarn, engine.config, 'reduce')
    for target_resource_type, source_resource_type, handler in handlers:
        logger.info("full reduce resync source=%s target=%s handler=%r version=%s",
                    source_resource_type, target_resource_type, handler['handler'],
                    handler['version'])
        start = time.time()
        for keys in chunks(100, iter_reduce_resync_keys(qvarn, source_resource_type)):
            for key in keys:
                process_reduce(qvarn, engine.config, source_resource_type, key,
                               [(target_resource_type, handler)], resync=True)
            yield
        # Update handler version only when full resync is successfully done.
        update_handler_version(qvarn, instance, target_resource_type, source_resource_type,
                               handler['version'])
        logger.info("done full reduce resync source=%s target=%s handler=%r version=%s time=%.2fs",
                    source_resource_type, target_resource_type, handler['handler'],
                    handler['version'], time.time() - start)
