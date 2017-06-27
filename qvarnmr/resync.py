from qvarnmr.clients.qvarn import QvarnApi
from qvarnmr.processor import UPDATED, process_changes, process_reduce, get_handlers
from qvarnmr.utils import chunks


def _no_action():
    pass


def iter_map_resync_changes(qvarn: QvarnApi, source_resource_type: str):
    for resource_id in qvarn.get_list(source_resource_type):
        yield source_resource_type, UPDATED, resource_id, _no_action


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


def resync_changed_handlers(qvarn: QvarnApi, config: dict, instance: str):
    mappers, reducers = get_handlers(config)

    # First resync all map handlers.
    handlers = iter_changed_handlers(qvarn, config, 'map')
    for target_resource_type, source_resource_type, handler in handlers:
        for changes in chunks(100, iter_map_resync_changes(qvarn, source_resource_type)):
            process_changes(qvarn, config, changes, mappers, reducers, resync=True)
            yield
        # Update handler version only when full resync is successfully done.
        update_handler_version(qvarn, instance, target_resource_type, source_resource_type,
                               handler['version'])

    # Resync reduce handlers separately, because in order to resync reduce handlers, we don't need
    # to resync map handlers. And by the way, `process_changes` automatically calls
    # `process_reduce`, so here we might have some duplication if both, map and related reduce
    # handlers where updated. That is something, that could be optimized.
    handlers = iter_changed_handlers(qvarn, config, 'reduce')
    for target_resource_type, source_resource_type, handler in handlers:
        for keys in chunks(100, iter_reduce_resync_keys(qvarn, source_resource_type)):
            for key in keys:
                process_reduce(qvarn, config, source_resource_type, key,
                               [(target_resource_type, handler)], resync=True)
        # Update handler version only when full resync is successfully done.
        update_handler_version(qvarn, instance, target_resource_type, source_resource_type,
                               handler['version'])
