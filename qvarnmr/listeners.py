import os
import socket
import datetime
import logging

from collections import namedtuple

from qvarnmr.constants import DATETIME_FORMAT
from qvarnmr.exceptions import BusyListenerError
from qvarnmr.validation import validate_handlers

logger = logging.getLogger(__name__)

Listener = namedtuple('Listener', ('source_resource_type', 'listener', 'state'))


def get_or_create_listeners(qvarn, instance: str, config: dict):
    validate_handlers(config)

    listeners = []
    sources = set()
    for target_resource_type, handlers in config.items():
        for source_resource_type, handler in handlers.items():
            # Make sure we have one listener, per source.
            if source_resource_type in sources:
                continue
            sources.add(source_resource_type)

            state = qvarn.search_one(
                'qvarnmr_listeners',
                instance=instance,
                resource_type=source_resource_type,
                default=None,
            )

            if state is None:
                listener = qvarn.create(source_resource_type + '/listeners', {
                    'notify_of_new': True,
                    'listen_on_all': True,
                })
                state = qvarn.create('qvarnmr_listeners', {
                    'instance': instance,
                    'resource_type': source_resource_type,
                    'listener_id': listener['id'],
                    'timestamp': None,
                    'owner': None,
                })
            else:
                listener = qvarn.get(source_resource_type + '/listeners', state['listener_id'])

            listeners.append(Listener(source_resource_type, listener, state))

    return listeners


def check_and_update_listeners_state(qvarn, listeners: list, interval: float=10, timeout: float=30):
    """Try to update listener state and fail if another worker uses a listener.

    Parameters
    ----------
    qvarn : qvarnmr.clients.qvarn.QvarnApi
    listeners : List[Listener]
        List of listeners to check and updated.
    interval : int or float
        Interval between updates in seconds. When worker is running it have to update
        listener.state.timestamp in specified intervals for other concurent workers to know if it is
        taken or not.
    timeout : int or float
        Timeout in seconds. If listener.state.timestamp was not updated more than timeout, then it
        is assumed, that this listener is no longer owned by a worker.

    Returns
    -------
    List[Listener]
        Returns updated list of listeners.

    """
    result = []
    interval = datetime.timedelta(seconds=interval)
    timeout = datetime.timedelta(seconds=timeout)
    signature = get_worker_signature()

    for listener in listeners:
        now = datetime.datetime.utcnow()
        state = listener.state
        timestamp = (now if state['timestamp'] is None else
                     datetime.datetime.strptime(state['timestamp'], DATETIME_FORMAT))

        # If more than <timeout> time has passed, fetch state from Qvarn because it could be changed
        # by another process.
        if now - timestamp >= timeout:
            logger.warning("refresh state resource timeout=%.2fs time=%.2fs",
                           timeout.total_seconds(), (now - timestamp).total_seconds())
            state = qvarn.get('qvarnmr_listeners', state['id'])
            timestamp = (now if state['timestamp'] is None else
                         datetime.datetime.strptime(state['timestamp'], DATETIME_FORMAT))

        owner = state['owner'] or signature

        outdated = owner == signature and now - timestamp > interval
        timedout = owner != signature and now - timestamp > timeout
        busy = owner != signature and now - timestamp <= timeout

        if outdated or timedout or state['timestamp'] is None or state['owner'] is None:
            logger.debug('update keep alive state signature=%s owner=%s time=%s timeout=%s '
                         'interval=%s timestamp=%s', signature, owner,
                         (now - timestamp).total_seconds(), timeout.total_seconds(),
                         interval.total_seconds(), now.strftime(DATETIME_FORMAT))
            state = qvarn.update('qvarnmr_listeners', state['id'], dict(
                state,
                owner=signature,
                timestamp=now.strftime(DATETIME_FORMAT),
            ))
        elif busy:
            logger.error('map/reduce database has busy state signature=%s owner=%s time=%s '
                         'timeout=%s interval=%s', signature, owner,
                         (now - timestamp).total_seconds(), timeout.total_seconds(),
                         interval.total_seconds())
            raise BusyListenerError("map/reduce engine is already running on %s" % owner)

        result.append(listener._replace(state=state))

    return result


def clear_listener_owners(qvarn, listeners: list):
    """Clear owner from all given listeners.

    When owner is cleared, other owners can run worker immediately, without waiting for timeout.

    Parameters
    ----------
    qvarn : qvarnmr.clients.qvarn.QvarnApi
    listeners : List[Listener]
        List of listeners to check and updated. This list most likely will be returned by
        ``get_or_create_listeners`` or ``check_and_update_listeners_state`` functions.

    Returns
    -------
    List[Listener]
        Returns updated list of listeners.

    """
    result = []
    for listener in listeners:
        state = qvarn.get('qvarnmr_listeners', listener.state['id'])
        state = qvarn.update('qvarnmr_listeners', state['id'], dict(
            state,
            owner=None,
            timestamp=datetime.datetime.utcnow().strftime(DATETIME_FORMAT),
        ))
        result.append(listener._replace(state=state))
    return result


def get_worker_signature():
    return '%s/%s' % (socket.gethostname(), os.getpid())
