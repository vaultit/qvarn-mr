import argparse

from qvarnmr.config import get_config, set_config
from qvarnmr.clients.qvarn import QvarnApi, setup_qvarn_client
from qvarnmr.listeners import get_or_create_listeners
from qvarnmr.handlers import import_handlers_config
from qvarnmr.resync import resync


def main(argv: list=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('handlers', help="python dotted path to map/reduce handlers config")
    parser.add_argument('resource_types', nargs='?', help=(
        "comma separated list of resource types to resync, if not specified "
        "all resource types will be resynced"
    ))
    parser.add_argument('-c', '--config', required=True, help="app config file")
    args = parser.parse_args(argv)

    set_config(args.config)
    config = get_config()

    client = setup_qvarn_client(config)
    qvarn = QvarnApi(client)

    handlers = import_handlers_config(args.handlers)

    # Make sure, that Qvarn listeners are created.
    listeners = get_or_create_listeners(qvarn, config['qvarnmr']['instance'], handlers)

    if args.resource_types:
        resource_types = [x.strip() for x in args.resource_types.split(',')]
    else:
        resource_types = [x for x, _ in listeners]

    for resource_type in resource_types:
        resync(qvarn, handlers, resource_type)


if __name__ == "__main__":
    main()  # pragma: no cover
