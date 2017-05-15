import argparse
import importlib

from qvarnmr.config import get_config, set_config
from qvarnmr.processor import UPDATED, DELETED, process_changes, get_handlers
from qvarnmr.clients.qvarn import QvarnApi, setup_qvarn_client


def import_handlers_config(path: str):
    module_name, config_variable_name = path.rsplit('.', 1)
    module = importlib.import_module(module_name)
    return getattr(module, config_variable_name)


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


def main(argv: list=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('handlers', help="python dotted path to map/reduce handlers config")
    parser.add_argument('resource_type', help="resource type to resync")
    parser.add_argument('-c', '--config', help="app config file")
    args = parser.parse_args(argv)

    set_config(args.config)
    config = get_config()

    client = setup_qvarn_client(config)
    qvarn = QvarnApi(client)

    config = import_handlers_config(args.handlers)
    resync(qvarn, config, args.resource_type)


if __name__ == "__main__":
    main()
