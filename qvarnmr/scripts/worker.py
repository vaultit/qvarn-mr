import argparse
import time

from qvarnmr.config import get_config, set_config
from qvarnmr.clients.qvarn import QvarnApi, setup_qvarn_client
from qvarnmr.handlers import import_handlers_config, get_handlers
from qvarnmr.processor import get_changes, process_changes
from qvarnmr.listeners import get_or_create_listeners
from qvarnmr.resync import resync_changed_handlers


def main(argv: list=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('handlers', help="python dotted path to map/reduce handlers config")
    parser.add_argument('-c', '--config', required=True, help="app config file")
    parser.add_argument('-f', '--forever', action='store_true', default=False, help="process changes forever")
    args = parser.parse_args(argv)

    set_config(args.config)
    config = get_config()

    client = setup_qvarn_client(config)
    qvarn = QvarnApi(client)

    handlers = import_handlers_config(args.handlers)
    listeners = get_or_create_listeners(qvarn, config['qvarnmr']['instance'], handlers)
    mappers, reducers = get_handlers(handlers)

    # Do automatic full resync for new or changed map/reduce handlers.
    for _ in resync_changed_handlers(qvarn, handlers, config['qvarnmr']['instance']):
        # We don't want to suspend whole map/reduce engine while full resync is in progress. That is
        # why, we continue to process newest changes, while full resync is in progress.
        # TODO: we can't process changes in parallel to full resync, because checking for outdated
        #       map items passed to reduce is not yet implemented.
        #       Uncomment two lines below and remove `pass` once this is done.
        #       For now, parallel processing of changes is disabled.
        # changes = get_changes(qvarn, listeners)
        # process_changes(qvarn, changes, mappers, reducers)
        pass

    # Watch notifications and process map/reduce handlers forever.
    while True:
        changes = get_changes(qvarn, listeners)
        changes_processed = process_changes(qvarn, changes, mappers, reducers)

        if args.forever:
            # If no changes was processed go into sleep mode and wait a few
            # moments before checking for more changes.
            if changes_processed == 0:
                time.sleep(0.5)
        else:
            break


if __name__ == "__main__":
    main()  # pragma: no cover
