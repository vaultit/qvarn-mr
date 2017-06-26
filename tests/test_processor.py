from qvarnmr.processor import UPDATED
from qvarnmr.processor import process_map, process
from qvarnmr.func import item
from qvarnmr.handlers import get_handlers
from qvarnmr.testing.utils import get_mapped_data, get_reduced_data
from qvarnmr.listeners import get_or_create_listeners


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
                },
            },
        ],
    },
}


def test_process_map_resync_samever(pretender, qvarn):
    pretender.add_resource_types(SCHEMA)

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

    # Process map handlers in resync mode, since map handler version matches one already saved,
    # nothing should be done.
    mappers, reducers = get_handlers(config)
    assert list(process_map(qvarn, 'source', UPDATED, data['id'], mappers['source'],
                            resync=True)) == []

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


def test_delete_reduce_key_if_source_is_empty(pretender, qvarn):
    pretender.add_resource_types(SCHEMA)

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
    reduced = get_reduced_data(qvarn, 'reduce_target', 1)
    assert reduced[key]['_mr_value'] == 5

    # Remove source resource with value = 2, reduce value should be 3.
    qvarn.delete('source', data[0]['id'])
    process(qvarn, listeners, config)
    reduced = get_reduced_data(qvarn, 'reduce_target', 1)
    assert reduced[key]['_mr_value'] == 3

    # Finally remove last source resource, reduce resource should be deleted too.
    qvarn.delete('source', data[1]['id'])
    process(qvarn, listeners, config)
    reduced = get_reduced_data(qvarn, 'reduce_target')
    assert reduced == {}
