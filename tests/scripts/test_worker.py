from io import StringIO
from copy import deepcopy

from qvarnmr.scripts import worker
from qvarnmr.func import item, value
from qvarnmr.testing.utils import get_reduced_data, get_resource_values, update_resource


SCHEMA = {
    'source': {
        'path': '/source',
        'type': 'source',
        'versions': [
            {
                'version': 'v1',
                'prototype': {
                    'id': '',
                    'type': '',
                    'revision': '',
                    'key': 0,
                    'value': 0,
                },
            },
        ],
    },
    'map_target': {
        'path': '/map_target',
        'type': 'map_target',
        'versions': [
            {
                'version': 'v1',
                'prototype': {
                    'id': '',
                    'type': '',
                    'revision': '',
                    '_mr_key': 0,
                    '_mr_value': 0,
                    '_mr_source_id': '',
                    '_mr_source_type': '',
                    '_mr_version': 0,
                    '_mr_deleted': 0,
                },
            },
        ],
    },
    'reduce_target': {
        'path': '/reduce_target',
        'type': 'reduce_target',
        'versions': [
            {
                'version': 'v1',
                'prototype': {
                    'id': '',
                    'type': '',
                    'revision': '',
                    '_mr_key': 0,
                    '_mr_value': 0,
                    '_mr_version': 0,
                },
            },
        ],
    },
}

CONFIG = {
    'map_target': {
        'source': {
            'type': 'map',
            'version': 1,
            'handler': item('key', 'value'),
        },
    },
    'reduce_target': {
        'map_target': {
            'type': 'reduce',
            'version': 1,
            'handler': sum,
            'map': value(),
        },
    },
}


def test_worker(pretender, qvarn, mocker, config):
    mocker.patch('qvarnmr.scripts.worker.set_config')
    mocker.patch('qvarnmr.scripts.worker.setup_qvarn_client', return_value=qvarn.client)

    pretender.add_resource_types(SCHEMA)

    mocker.patch('qvarnmr.testing.config', CONFIG, create=True)

    # Create notifications.
    worker.main(['qvarnmr.testing.config', '-c', 'qvarnmr.cfg'])

    # Create several resources.
    resources = [
        qvarn.create('source', {'key': 1, 'value': 1}),
        qvarn.create('source', {'key': 1, 'value': 2}),
        qvarn.create('source', {'key': 1, 'value': 3}),
    ]
    worker.main(['qvarnmr.testing.config', '-c', 'qvarnmr.cfg'])
    assert get_resource_values(qvarn, 'reduce_target', ('_mr_key', '_mr_value')) == [(1, 6)]

    # Update some resources.
    qvarn.update('source', resources[0]['id'], {'key': 1, 'value': 2})
    qvarn.update('source', resources[2]['id'], {'key': 1, 'value': 5})
    worker.main(['qvarnmr.testing.config', '-c', 'qvarnmr.cfg'])
    assert get_resource_values(qvarn, 'reduce_target', ('_mr_key', '_mr_value')) == [(1, 9)]

    # Delete some resources.
    qvarn.delete('source', resources[2]['id'])
    worker.main(['qvarnmr.testing.config', '-c', 'qvarnmr.cfg'])
    assert get_resource_values(qvarn, 'reduce_target', ('_mr_key', '_mr_value')) == [(1, 4)]


def test_auto_resync(pretender, qvarn, mocker, config):
    mocker.patch('qvarnmr.scripts.worker.set_config')
    mocker.patch('qvarnmr.scripts.worker.setup_qvarn_client', return_value=qvarn.client)

    pretender.add_resource_types(SCHEMA)

    config_ = deepcopy(CONFIG)
    mocker.patch('qvarnmr.testing.config', config_, create=True)

    # Create several resources.
    qvarn.create('source', {'key': 1, 'value': 1}),
    qvarn.create('source', {'key': 1, 'value': 2}),
    qvarn.create('source', {'key': 1, 'value': 3}),

    # Run worker, it should do automatic synchronisation of all the data.
    worker.main(['qvarnmr.testing.config', '-c', 'qvarnmr.cfg'])
    reduced = get_reduced_data(qvarn, 'reduce_target', 1)
    assert reduced[1]['_mr_value'] == 6

    def new_map_handler(resource):
        return resource['key'], resource['value'] * 2

    # Change handler and version, that should be picked up by worker automatically and all data
    # should be resynced with new handler.
    config_['map_target']['source'] = {
        'type': 'map',
        'version': 2,
        'handler': new_map_handler,
    }
    worker.main(['qvarnmr.testing.config', '-c', 'qvarnmr.cfg'])
    reduced = get_reduced_data(qvarn, 'reduce_target', 1)
    assert reduced[1]['_mr_value'] == 12

    # Lets try  to change the reduce handler. Automatic resync should happen too.
    config_['reduce_target']['map_target'] = {
        'type': 'reduce',
        'version': 2,
        'handler': min,
        'map': value(),
    }
    worker.main(['qvarnmr.testing.config', '-c', 'qvarnmr.cfg'])
    reduced = get_reduced_data(qvarn, 'reduce_target', 1)
    assert reduced[1]['_mr_value'] == 2


def test_check_for_running_workers(pretender, qvarn, mocker, config):
    mocker.patch('qvarnmr.scripts.worker.set_config')
    mocker.patch('qvarnmr.scripts.worker.setup_qvarn_client', return_value=qvarn.client)
    mocker.patch('qvarnmr.testing.config', CONFIG, create=True)

    pretender.add_resource_types(SCHEMA)

    # Run worker from host1, process all changes and quit.
    mocker.patch('socket.gethostname', return_value='host1')
    mocker.patch('os.getpid', return_value=1)
    assert worker.main(['qvarnmr.testing.config', '-c', 'qvarnmr.cfg']) is None

    # Run worker from host2, lock from host2 should be released.
    mocker.patch('socket.gethostname', return_value='host2')
    assert worker.main(['qvarnmr.testing.config', '-c', 'qvarnmr.cfg']) is None

    # Run worker from host3 while owner is still set.
    output = mocker.patch('sys.stdout', StringIO())
    mocker.patch('socket.gethostname', return_value='host3')
    update_resource(qvarn, 'qvarnmr_listeners', resource_type='source')(owner='host3')
    assert worker.main(['qvarnmr.testing.config', '-c', 'qvarnmr.cfg']) == 1
    assert output.getvalue() == 'map/reduce engine is already running on host3\n'
