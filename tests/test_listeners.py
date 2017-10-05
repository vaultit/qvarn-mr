import pytest

from operator import itemgetter

from qvarnmr.testing.utils import get_resource_values
from qvarnmr.exceptions import BusyListenerError
from qvarnmr.func import item, count
from qvarnmr.listeners import (
    get_or_create_listeners,
    check_and_update_listeners_state,
    clear_listener_owners,
)


SCHEMA = {
    'data1': {
        'path': '/data1',
        'type': 'data1',
        'versions': [
            {
                'version': 'v1',
                'prototype': {
                    'id': '',
                    'type': '',
                    'revision': '',
                    'value': '',
                },
            },
        ],
    },
    'data2': {
        'path': '/data2',
        'type': 'data2',
        'versions': [
            {
                'version': 'v1',
                'prototype': {
                    'id': '',
                    'type': '',
                    'revision': '',
                    'value': '',
                },
            },
        ],
    },
    'data__map': {
        'path': '/data__map',
        'type': 'data__map',
        'versions': [
            {
                'version': 'v1',
                'prototype': {
                    'type': '',
                    'id': '',
                    'revision': '',
                    '_mr_key': '',
                    '_mr_value': '',
                    '_mr_source_id': '',
                    '_mr_source_type': '',
                    '_mr_version': 0,
                    '_mr_deleted': False,
                },
            },
        ],
    },
}


def test_get_listeners(realqvarn, qvarn):
    realqvarn.add_resource_types(SCHEMA)

    config = {
        'data__map': {
            'data1': {
                'type': 'map',
                'version': 1,
                'handler': item('id'),
            },
            'data2': {
                'type': 'map',
                'version': 1,
                'handler': item('id'),
            },
        },
        'data__join': {
            'data__map': {
                'type': 'reduce',
                'version': 1,
                'handler': count,
            },
        },
    }

    data1_listener = qvarn.create('data1/listeners', {
        'notify_of_new': True,
        'listen_on_all': True,
    })
    qvarn.create('qvarnmr_listeners', {
        'instance': 'test',
        'resource_type': 'data1',
        'listener_id': data1_listener['id'],
    })

    listeners = get_or_create_listeners(qvarn, 'test', config)

    data2_qvarnmr_listener = qvarn.search_one('qvarnmr_listeners', instance='test',
                                              resource_type='data2')
    data2_listener = qvarn.get('data2/listeners', data2_qvarnmr_listener['listener_id'])

    data__map_qvarnmr_listener = qvarn.search_one('qvarnmr_listeners', instance='test',
                                                  resource_type='data__map')
    data__map_listener = qvarn.get('data__map/listeners', data__map_qvarnmr_listener['listener_id'])

    result = [
        (
            x.source_resource_type,
            x.listener,
            x.state['owner'],
            x.state['timestamp'],
        )
        for x in sorted(listeners, key=itemgetter(0))
    ]
    assert result == [
        ('data1', data1_listener, None, None),
        ('data2', data2_listener, None, None),
        ('data__map', data__map_listener, None, None),
    ]

    assert len(qvarn.get_list('data1/listeners')) == 1
    assert len(qvarn.get_list('data2/listeners')) == 1


def test_check_and_update_listeners_state(realqvarn, qvarn, freezetime, mocker):
    realqvarn.add_resource_types(SCHEMA)

    config = {
        'data__map': {
            'data1': {
                'type': 'map',
                'version': 1,
                'handler': item('id'),
            },
        },
        'data2': {
            'data1': {
                'type': 'map',
                'version': 1,
                'handler': item('id'),
            },
        }
    }

    mocker.patch('socket.gethostname', return_value='hostname')
    mocker.patch('os.getpid', return_value=1)

    # Initial listeners discovery, <owner> and <timestamp> should not be set yet.
    freezetime('2017-07-12 00:00:00')
    listeners = get_or_create_listeners(qvarn, 'test', config)
    assert get_resource_values(qvarn, 'qvarnmr_listeners', ('owner', 'timestamp')) == [
        (None, None),
    ]

    # First check and update, should do the update.
    freezetime('2017-07-12 00:00:05')
    listeners = check_and_update_listeners_state(qvarn, listeners, interval=10, timeout=60)
    assert get_resource_values(qvarn, 'qvarnmr_listeners', ('owner', 'timestamp')) == [
        ('hostname/1', '2017-07-12T00:00:05.000000'),
    ]

    # If less time than <interval> passed and <owner> is the same, nothing should be done.
    freezetime('2017-07-12 00:00:10')
    listeners = check_and_update_listeners_state(qvarn, listeners, interval=10, timeout=60)
    assert get_resource_values(qvarn, 'qvarnmr_listeners', ('owner', 'timestamp')) == [
        ('hostname/1', '2017-07-12T00:00:05.000000'),
    ]

    # More than <interval> time passed, listener should be updated.
    freezetime('2017-07-12 00:00:20')
    listeners = check_and_update_listeners_state(qvarn, listeners, interval=10, timeout=60)
    assert get_resource_values(qvarn, 'qvarnmr_listeners', ('owner', 'timestamp')) == [
        ('hostname/1', '2017-07-12T00:00:20.000000'),
    ]

    # More than <timeout> time passed, listener should be updated.
    freezetime('2017-07-12 00:01:30')
    listeners = check_and_update_listeners_state(qvarn, listeners, interval=10, timeout=60)
    assert get_resource_values(qvarn, 'qvarnmr_listeners', ('owner', 'timestamp')) == [
        ('hostname/1', '2017-07-12T00:01:30.000000'),
    ]

    # Host name has changed and <timestamp> was updated less than <timeout> time ago.
    freezetime('2017-07-12 00:01:35')
    mocker.patch('socket.gethostname', return_value='hostname2')
    with pytest.raises(BusyListenerError) as e:
        check_and_update_listeners_state(qvarn, listeners, interval=10, timeout=60)
    assert str(e.value) == 'map/reduce engine is already running on hostname/1'
    assert get_resource_values(qvarn, 'qvarnmr_listeners', ('owner', 'timestamp')) == [
        ('hostname/1', '2017-07-12T00:01:30.000000'),
    ]

    # Host name has changed and <timestamp> was updated more than <timeout> time ago.
    freezetime('2017-07-12 00:03:00')
    listeners = check_and_update_listeners_state(qvarn, listeners, interval=10, timeout=60)
    assert get_resource_values(qvarn, 'qvarnmr_listeners', ('owner', 'timestamp')) == [
        ('hostname2/1', '2017-07-12T00:03:00.000000'),
    ]

    # After clearing owners, other workers can pick up listeners.
    freezetime('2017-07-12 00:03:01')
    mocker.patch('socket.gethostname', return_value='hostname3')
    listeners = clear_listener_owners(qvarn, listeners)
    assert get_resource_values(qvarn, 'qvarnmr_listeners', ('owner', 'timestamp')) == [
        (None, '2017-07-12T00:03:01.000000'),
    ]

    check_and_update_listeners_state(qvarn, listeners, interval=10, timeout=60)
    assert get_resource_values(qvarn, 'qvarnmr_listeners', ('owner', 'timestamp')) == [
        ('hostname3/1', '2017-07-12T00:03:01.000000'),
    ]


def test_check_and_update_listeners_state_changed_revision_case(realqvarn, qvarn, freezetime,
                                                                mocker):
    realqvarn.add_resource_types(SCHEMA)

    config = {
        'data__map': {
            'data1': {
                'type': 'map',
                'version': 1,
                'handler': item('id'),
            },
        },
    }

    mocker.patch('socket.gethostname', return_value='hostname')
    mocker.patch('os.getpid', return_value=1)

    # Initial listeners discovery, <owner> and <timestamp> should not be set yet.
    freezetime('2017-07-12 00:00:00')
    listeners = get_or_create_listeners(qvarn, 'test', config)
    listeners = check_and_update_listeners_state(qvarn, listeners, interval=10, timeout=60)
    assert get_resource_values(qvarn, 'qvarnmr_listeners', ('owner', 'timestamp')) == [
        ('hostname/1', '2017-07-12T00:00:00.000000'),
    ]

    # More than <timeout> time has passed, and state has been changed by another process.
    freezetime('2017-07-12 00:01:10')
    mocker.patch('os.getpid', return_value=2)
    check_and_update_listeners_state(qvarn, listeners, interval=10, timeout=60)

    freezetime('2017-07-12 00:01:30')
    mocker.patch('os.getpid', return_value=1)
    with pytest.raises(BusyListenerError) as e:
        check_and_update_listeners_state(qvarn, listeners, interval=10, timeout=60)
    assert str(e.value) == 'map/reduce engine is already running on hostname/2'
    assert get_resource_values(qvarn, 'qvarnmr_listeners', ('owner', 'timestamp')) == [
        ('hostname/2', '2017-07-12T00:01:10.000000'),
    ]
