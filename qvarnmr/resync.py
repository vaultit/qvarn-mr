from qvarnmr.clients.qvarn import QvarnApi
from qvarnmr.processor import UPDATED, DELETED, process_changes, get_handlers


def find_unprocessed_ids(qvarn, resource_type, mappers, processed_ids):
    for target_resource_type, mapper in mappers[resource_type]:
        source_ids = {
            resource['_mr_source_id']
            for resource in qvarn.search(target_resource_type, show=['_mr_source_id'])
        }
        for resource_id in (source_ids - processed_ids):
            yield resource_type, DELETED, resource_id


def resync(qvarn: QvarnApi, config: dict, resource_type: str):
    mappers, reducers = get_handlers(config)
    resource_ids = set(qvarn.get_list(resource_type))
    changes = ((resource_type, UPDATED, resource_id) for resource_id in resource_ids)
    process_changes(qvarn, changes, mappers, reducers)

    # Find derived resources, that have source ids that does not exist in the
    # list source ids.
    changes = find_unprocessed_ids(qvarn, resource_type, mappers, resource_ids)
    process_changes(qvarn, changes, mappers, reducers)
