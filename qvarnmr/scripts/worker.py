import argparse
import time
import sys
import logging
import datetime

from qvarnmr.config import get_config, set_config
from qvarnmr.clients.qvarn import QvarnApi, setup_qvarn_client
from qvarnmr.handlers import import_handlers_config
from qvarnmr.processor import MapReduceEngine, get_changes
from qvarnmr.resync import resync_changed_handlers
from qvarnmr.exceptions import BusyListenerError
from qvarnmr.listeners import (
    get_or_create_listeners,
    check_and_update_listeners_state,
    clear_listener_owners,
    get_worker_signature,
)


LISTENER_UPDATE_INTERVAL = 10  # seconds
LISTENER_TIMEOUT = 60  # seconds

logger = logging.getLogger(__name__)


def main(argv: list=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('handlers', help="python dotted path to map/reduce handlers config")
    parser.add_argument('-c', '--config', required=True, help="app config file")
    parser.add_argument('-f', '--forever', action='store_true', default=False, help="process changes forever")
    args = parser.parse_args(argv)

    now = datetime.datetime.utcnow()
    logger.info("starting map/reduce worker on %s at %s", get_worker_signature(), now.isoformat())

    set_config(args.config)
    config = get_config()

    client = setup_qvarn_client(config)
    qvarn = QvarnApi(client)

    listeners = None

    try:
        handlers = import_handlers_config(args.handlers)
        engine = MapReduceEngine(qvarn, handlers)

        listeners = get_or_create_listeners(qvarn, config['qvarnmr']['instance'], handlers)

        def keep_alive():
            nonlocal listeners
            listeners = check_and_update_listeners_state(
                qvarn, listeners,
                interval=config.getfloat('qvarnmr', 'keep_alive_update_interval',
                                         fallback=LISTENER_UPDATE_INTERVAL),
                timeout=config.getfloat('qvarnmr', 'keep_alive_timeout', fallback=LISTENER_TIMEOUT),
            )

        # Immediately check if another map/reduce processor is not running.
        keep_alive()

        for event in ('map_handler_processed', 'reduce_handler_processed'):
            engine.add_callback(event, keep_alive)

        # Do automatic full resync for new or changed map/reduce handlers.
        for _ in resync_changed_handlers(qvarn, engine, config['qvarnmr']['instance']):
            # We don't want to suspend whole map/reduce engine while full resync is in progress.
            # That is why, we continue to process newest changes, while full resync is in progress.
            changes = get_changes(qvarn, listeners)
            engine.process_changes(changes)

        logger.info("entering the main loop")

        # Watch notifications and process map/reduce handlers forever.
        while True:
            changes = get_changes(qvarn, listeners)
            changes_processed = engine.process_changes(changes)

            if args.forever:
                if changes_processed == 0:
                    # If no changes were processed go into sleep mode and wait a few moments before
                    # checking for more changes.
                    time.sleep(0.5)
                    keep_alive()
            elif changes_processed == 0:
                # If forever flag is not set, wait until all pending changes are processed and then
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
