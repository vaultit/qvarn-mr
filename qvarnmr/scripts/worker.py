import argparse
import time
import sys

from qvarnmr.config import get_config, set_config
from qvarnmr.clients.qvarn import QvarnApi, setup_qvarn_client
from qvarnmr.handlers import import_handlers_config, get_handlers
from qvarnmr.processor import get_changes, process_changes
from qvarnmr.resync import resync_changed_handlers
from qvarnmr.exceptions import BusyListenerError
from qvarnmr.listeners import (
    get_or_create_listeners,
    check_and_update_listeners_state,
    clear_listener_owners,
)


LISTENER_UPDATE_INTERVAL = 10  # seconds
LISTENER_TIMEOUT = 60  # seconds


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

    listeners = None

    try:
        handlers = import_handlers_config(args.handlers)
        listeners = get_or_create_listeners(qvarn, config['qvarnmr']['instance'], handlers)
        listeners = check_and_update_listeners_state(qvarn, listeners,
                                                     interval=LISTENER_UPDATE_INTERVAL,
                                                     timeout=LISTENER_TIMEOUT)
        mappers, reducers = get_handlers(handlers)

        # Do automatic full resync for new or changed map/reduce handlers.
        for _ in resync_changed_handlers(qvarn, handlers, config['qvarnmr']['instance']):
            # We don't want to suspend whole map/reduce engine while full resync is in progress.
            # That is why, we continue to process newest changes, while full resync is in progress.
            changes = get_changes(qvarn, listeners)
            process_changes(qvarn, handlers, changes, mappers, reducers)
            listeners = check_and_update_listeners_state(qvarn, listeners,
                                                         interval=LISTENER_UPDATE_INTERVAL,
                                                         timeout=LISTENER_TIMEOUT)

        # Watch notifications and process map/reduce handlers forever.
        while True:
            changes = get_changes(qvarn, listeners)
            changes_processed = process_changes(qvarn, handlers, changes, mappers, reducers)
            listeners = check_and_update_listeners_state(qvarn, listeners,
                                                         interval=LISTENER_UPDATE_INTERVAL,
                                                         timeout=LISTENER_TIMEOUT)

            if args.forever:
                if changes_processed == 0:
                    # If no changes were processed go into sleep mode and wait a few moments before
                    # checking for more changes.
                    time.sleep(0.5)
            elif changes_processed == 0:
                # If forever flag is not set, wait untill all pending changes are processed and then
                # exit the loop.
                break

    except BusyListenerError as e:
        print(e)
        return 1

    except:
        if listeners:
            clear_listener_owners(qvarn, listeners)
        raise

    else:
        clear_listener_owners(qvarn, listeners)


if __name__ == "__main__":
    sys.exit(main() or 0)  # pragma: no cover
