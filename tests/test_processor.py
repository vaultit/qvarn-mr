from qvarnmr.processor import UPDATED
from qvarnmr.processor import _process_map, MapReduceEngine
from qvarnmr.func import item, value
from qvarnmr.handlers import get_handlers
from qvarnmr.listeners import get_or_create_listeners, check_and_update_listeners_state
from qvarnmr.testing.utils import get_mapped_data, get_resource_values, update_resource, process


SCHEMA = {
    'source': {
        'path': '/source',
        'type': 'source',
        'versions': [
            {
                'version': 'v1',
                'prototype': {
                    'type': '',
                    'id': '',
                    'revision': '',
                    'key': '',
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
                    'type': '',
                    'id': '',
                    'revision': '',
                    '_mr_key': '',
                    '_mr_value': 0,
                    '_mr_source_id': '',
                    '_mr_source_type': '',
                    '_mr_version': 0,
                    '_mr_deleted': False,
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
                    'type': '',
                    'id': '',
                    'revision': '',
                    '_mr_key': '',
                    '_mr_value': 0,
                    '_mr_version': 0,
                    '_mr_timestamp': 0,
                },
            },
        ],
    },
}


def test_process_map_resync_samever(realqvarn, qvarn):
    realqvarn.add_resource_types(SCHEMA)

    config = {
        'map_target': {
            'source': {
                'type': 'map',
                'version': 1,
                'handler': item('id', 'value'),
            },
        },
    }

    data = qvarn.create('source', {'value': 1})

    # Create target resource with same version as specified for handler.
    qvarn.create('map_target', {
        '_mr_key': data['id'],
        '_mr_value': data['value'],
        '_mr_source_id': data['id'],
        '_mr_source_type': 'source',
        '_mr_version': 1,
    })

    # Process map handlers in resync mode, since map handler version matches the one already saved,
    # nothing should be done.
    mappers, reducers = get_handlers(config)
    assert _process_map(qvarn, 'source', UPDATED, data['id'], mappers['source'], resync=True) == 0

    mapped = get_mapped_data(qvarn, 'map_target')
    assert mapped[data['id']] == {
        'type': 'map_target',
        '_mr_key': data['id'],
        '_mr_value': data['value'],
        '_mr_source_id': data['id'],
        '_mr_source_type': 'source',
        '_mr_version': 1,
        '_mr_deleted': None,
    }


def test_delete_reduce_key_if_source_is_empty(realqvarn, qvarn):
    realqvarn.add_resource_types(SCHEMA)

    def reduce_handler(resources):
        resources = qvarn.get_multiple('map_target', resources)
        resources = [qvarn.get(x['_mr_source_type'], x['_mr_source_id']) for x in resources]
        return sum(x['value'] for x in resources)

    config = {
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
                'handler': reduce_handler,
            }
        }
    }

    listeners = get_or_create_listeners(qvarn, 'test', config)

    # Add two source value 2 and 3.
    key = '1'
    data = [
        qvarn.create('source', {'key': key, 'value': 2}),
        qvarn.create('source', {'key': key, 'value': 3}),
    ]

    # Proces map/reduce and reduce value should be 5 (2 + 3 = 5).
    process(qvarn, listeners, config)
    assert get_resource_values(qvarn, 'map_target', '_mr_value') == [2, 3]
    assert get_resource_values(qvarn, 'reduce_target', '_mr_value') == [5]

    # Remove source resource with value = 2, reduce value should be 3.
    qvarn.delete('source', data[0]['id'])
    process(qvarn, listeners, config)
    assert get_resource_values(qvarn, 'map_target', '_mr_value') == [3]
    assert get_resource_values(qvarn, 'reduce_target', '_mr_value') == [3]

    # Finally remove last source resource, reduce resource should be deleted too.
    qvarn.delete('source', data[1]['id'])
    process(qvarn, listeners, config)
    assert get_resource_values(qvarn, 'map_target', '_mr_value') == []
    assert get_resource_values(qvarn, 'reduce_target', '_mr_value') == []


def test_reduce_half_synced_key(realqvarn, qvarn):
    realqvarn.add_resource_types(SCHEMA)

    config = {
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
            }
        }
    }

    listeners = get_or_create_listeners(qvarn, 'test', config)

    # Add two source values 2 and 3.
    data = [
        qvarn.create('source', {'key': '1', 'value': 2}),
        qvarn.create('source', {'key': '1', 'value': 3}),
        qvarn.create('source', {'key': '2', 'value': 1}),
    ]

    # Proces map/reduce handlers.
    process(qvarn, listeners, config)
    assert get_resource_values(qvarn, 'map_target', ('_mr_key', '_mr_value', '_mr_version')) == [
        ('1', 2, 1),
        ('1', 3, 1),
        ('2', 1, 1),
    ]
    assert get_resource_values(qvarn, 'reduce_target', ('_mr_key', '_mr_value')) == [
        ('1', 5),
        ('2', 1),
    ]

    # Update map handler to a new handler.
    config['map_target']['source']['version'] = 2
    config['map_target']['source']['handler'] = lambda r: (r['key'], r['value'] * 2)

    # Update a source resource.
    # Since not all map_target resources where updated with new handler, reduce processor should
    # skip all such sources.
    update_resource(qvarn, 'source', data[1]['id'])(value=4)
    process(qvarn, listeners, config)
    assert get_resource_values(qvarn, 'map_target', ('_mr_key', '_mr_value', '_mr_version')) == [
        ('1', 2, 1),
        ('1', 8, 2),
        ('2', 1, 1),
    ]
    assert get_resource_values(qvarn, 'reduce_target', ('_mr_key', '_mr_value')) == [
        ('1', 5),
        ('2', 1),
    ]

    # Update the other resource.
    update_resource(qvarn, 'source', data[0]['id'])(value=3)
    process(qvarn, listeners, config)
    assert get_resource_values(qvarn, 'map_target', ('_mr_key', '_mr_value', '_mr_version')) == [
        ('1', 6, 2),
        ('1', 8, 2),
        ('2', 1, 1),
    ]
    assert get_resource_values(qvarn, 'reduce_target', ('_mr_key', '_mr_value')) == [
        ('1', 14),
        ('2', 1),
    ]


def test_callbacks(realqvarn, qvarn, mocker, freezetime):
    realqvarn.add_resource_types(SCHEMA)

    mocker.patch('socket.gethostname', return_value='hostname')
    mocker.patch('os.getpid', return_value=1)

    config = {
        'map_target': {
            'source': {
                'type': 'map',
                'version': 1,
                'handler': item('key'),
            },
        },
    }

    engine = MapReduceEngine(qvarn, config)
    listeners = get_or_create_listeners(qvarn, 'test', config)

    def keep_alive():
        nonlocal listeners
        listeners = check_and_update_listeners_state(qvarn, listeners, interval=10, timeout=60)

    for event in ('map_handler_processed', 'reduce_handler_processed'):
        engine.add_callback(event, keep_alive)

    qvarn.create('source', {'key': '1', 'value': 2}),
    qvarn.create('source', {'key': '1', 'value': 3}),

    freezetime('2017-07-12 00:00:00')
    process(qvarn, listeners, engine)
    assert get_resource_values(qvarn, 'map_target', ('_mr_source_type', '_mr_key')) == [
        ('source', '1'),
        ('source', '1'),
    ]
    assert get_resource_values(qvarn, 'qvarnmr_listeners', ('owner', 'timestamp')) == [
        ('hostname/1', '2017-07-12T00:00:00.000000'),
    ]


def test_reduce_multiple_resources_for_signle_key(realqvarn, qvarn, mocker, freezetime):
    realqvarn.add_resource_types(SCHEMA)

    config = {
        'map_target': {
            'source': {
                'type': 'map',
                'version': 1,
                'handler': item('key'),
            },
        },
        'reduce_target': {
            'map_target': {
                'type': 'reduce',
                'version': 1,
                'handler': lambda x: len(list(x)),
            },
        },
    }

    engine = MapReduceEngine(qvarn, config, raise_errors=True)
    listeners = get_or_create_listeners(qvarn, 'test', config)

    qvarn.create('source', {'key': 'k1', 'value': 2}),
    qvarn.create('reduce_target', {'_mr_key': 'k1'}),
    qvarn.create('reduce_target', {'_mr_key': 'k1'}),

    process(qvarn, listeners, engine)

    assert get_resource_values(qvarn, 'reduce_target', ('_mr_key', '_mr_value')) == [
        ('k1', 1),
    ]
