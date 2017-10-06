import pytest

from functools import reduce
from operator import mul
from unittest import mock

from qvarnmr.func import join, item, count, value, mr_func
from qvarnmr.testing.utils import cleaned, get_resource_values, get_reduced_data, process
from qvarnmr.listeners import get_or_create_listeners
from qvarnmr.processor import MapReduceEngine


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
    'mapped': {
        'path': '/mapped',
        'type': 'mapped',
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
    'reduced': {
        'path': '/reduced',
        'type': 'reduced',
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
                    '_mr_timestamp': 0,
                },
            },
        ],
    },
}


def test_mapreduce(realqvarn, qvarn):
    realqvarn.add_resource_types({
        'company_reports__map': {
            'path': '/company_reports__map',
            'type': 'company_reports__map',
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
        'company_reports': {
            'path': '/company_reports',
            'type': 'company_report',
            'versions': [
                {
                    'version': 'v1',
                    'prototype': {
                        'type': '',
                        'id': '',
                        'revision': '',
                        '_mr_key': '',
                        '_mr_value': '',
                        '_mr_version': 0,
                        '_mr_timestamp': 0,
                        'org_id': '',
                        'report_id': '',
                    },
                },
            ],
        },
    })

    config = {
        'company_reports__map': {
            'orgs': {
                'type': 'map',
                'version': 1,
                'handler': item('id'),
            },
            'reports': {
                'type': 'map',
                'version': 1,
                'handler': item('org'),
            },
        },
        'company_reports': {
            'company_reports__map': {
                'type': 'reduce',
                'version': 1,
                'handler': join({
                    'org': {
                        'id': 'org_id',
                    },
                    'report': {
                        'id': 'report_id',
                    },
                }),
            },
        }
    }

    listeners = get_or_create_listeners(qvarn, 'test', config)

    org = qvarn.create('orgs', {'names': ['Orgtra']})
    reports = [
        qvarn.create('reports', {'org': org['id'], 'generated_timestamp': '1'}),
        qvarn.create('reports', {'org': org['id'], 'generated_timestamp': '2'}),
    ]

    process(qvarn, listeners, config)

    mapped = qvarn.get_list('company_reports__map')
    assert len(mapped) == 3
    mapped = qvarn.get_multiple('company_reports__map', mapped)
    mapped = {r['_mr_source_id']: r for r in mapped}
    assert cleaned(mapped[org['id']]) == {
        'type': 'company_reports__map',
        '_mr_key': org['id'],
        '_mr_value': None,
        '_mr_source_id': org['id'],
        '_mr_source_type': 'orgs',
        '_mr_version': 1,
        '_mr_deleted': 0,
    }
    assert cleaned(mapped[reports[0]['id']]) == {
        'type': 'company_reports__map',
        '_mr_key': org['id'],
        '_mr_value': None,
        '_mr_source_id': reports[0]['id'],
        '_mr_source_type': 'reports',
        '_mr_version': 1,
        '_mr_deleted': 0,
    }
    assert cleaned(mapped[reports[1]['id']]) == {
        'type': 'company_reports__map',
        '_mr_key': org['id'],
        '_mr_value': None,
        '_mr_source_id': reports[1]['id'],
        '_mr_source_type': 'reports',
        '_mr_version': 1,
        '_mr_deleted': 0,
    }

    assert get_reduced_data(qvarn, 'company_reports') == {
        org['id']: {
            'type': 'company_report',
            '_mr_key': org['id'],
            '_mr_value': None,
            '_mr_version': 1,
            'org_id': org['id'],
            'report_id': reports[1]['id'],
        }
    }


def test_reduce_scalar_value(realqvarn, qvarn):
    realqvarn.add_resource_types({
        'reports_counts__map': {
            'path': '/reports_counts__map',
            'type': 'reports_count__map',
            'versions': [
                {
                    'version': 'v1',
                    'prototype': {
                        'id': '',
                        'type': '',
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
        'reports_counts': {
            'path': '/reports_counts',
            'type': 'reports_count',
            'versions': [
                {
                    'version': 'v1',
                    'prototype': {
                        'id': '',
                        'type': '',
                        'revision': '',
                        '_mr_key': '',
                        '_mr_value': 0,
                        '_mr_version': 0,
                        '_mr_timestamp': 0,
                    },
                },
            ],
        },
    })

    config = {
        'reports_counts__map': {
            'reports': {
                'type': 'map',
                'version': 1,
                'handler': item('org'),
            },
        },
        'reports_counts': {
            'reports_counts__map': {
                'type': 'reduce',
                'version': 1,
                'handler': count,
            },
        },
    }

    listeners = get_or_create_listeners(qvarn, 'test', config)

    org = qvarn.create('orgs', {'names': ['Orgtra']})
    reports = [
        qvarn.create('reports', {'org': org['id']}),
        qvarn.create('reports', {'org': org['id']}),
        qvarn.create('reports', {'org': org['id']}),
    ]

    process(qvarn, listeners, config)

    reduced = qvarn.get_list('reports_counts')
    assert len(reduced) == 1
    reduced = qvarn.get('reports_counts', reduced[0])
    assert cleaned(reduced) == {
        'type': 'reports_count',
        '_mr_key': org['id'],
        '_mr_value': len(reports),
        '_mr_version': 1,
    }


def test_create_update_delete_flow(realqvarn, qvarn):
    realqvarn.add_resource_types({
        'data': {
            'path': '/data',
            'type': 'data',
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
        'data_mapped': {
            'path': '/data_mapped',
            'type': 'data_mapped',
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
                        '_mr_deleted': False,
                    },
                },
            ],
        },
        'data_reduced': {
            'path': '/data_reduced',
            'type': 'data_reduced',
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
                        '_mr_timestamp': 0,
                    },
                },
            ],
        },
    })

    config = {
        'data_mapped': {
            'data': {
                'type': 'map',
                'version': 1,
                'handler': item('key', 'value'),
            },
        },
        'data_reduced': {
            'data_mapped': {
                'type': 'reduce',
                'version': 1,
                'handler': sum,
                'map': value()
            },
        },
    }

    listeners = get_or_create_listeners(qvarn, 'test', config)

    # Create several resources.
    resources = [
        qvarn.create('data', {'key': 1, 'value': 1}),
        qvarn.create('data', {'key': 1, 'value': 2}),
        qvarn.create('data', {'key': 1, 'value': 3}),
    ]
    process(qvarn, listeners, config)
    assert get_resource_values(qvarn, 'data_reduced', ('_mr_key', '_mr_value')) == [
        (1, 6),
    ]

    # Update some resources.
    qvarn.update('data', resources[0]['id'], {'key': 1, 'value': 2})
    qvarn.update('data', resources[2]['id'], {'key': 1, 'value': 5})
    process(qvarn, listeners, config)
    reduced = qvarn.get_list('data_reduced')
    reduced = qvarn.get('data_reduced', reduced[0])
    assert reduced['_mr_value'] == 9 and reduced['_mr_key'] == 1

    # Delete some resources.
    qvarn.delete('data', resources[2]['id'])
    process(qvarn, listeners, config)
    reduced = qvarn.get_list('data_reduced')
    reduced = qvarn.get('data_reduced', reduced[0])
    assert reduced['_mr_value'] == 4 and reduced['_mr_key'] == 1


def test_mapper_handler_error(realqvarn, qvarn, mocker):
    realqvarn.add_resource_types(SCHEMA)

    config = {
        'mapped': {
            'source': {
                'type': 'map',
                'version': 1,
                'handler': mock.Mock(side_effect=[
                    iter([(1, 1)]),
                    ValueError('fake error 1'),
                    iter([(1, 3)]),
                    ValueError('fake error 2'),
                    iter([(1, 2)]),
                ]),
            },
        },
    }

    import logging
    logger = logging.getLogger('test')

    engine = MapReduceEngine(qvarn, config)

    listeners = get_or_create_listeners(qvarn, 'test', config)
    listenerid = {x.source_resource_type: x.listener['id'] for x in listeners}

    qvarn.create('source', {'key': 1, 'value': 1}),
    qvarn.create('source', {'key': 1, 'value': 2}),
    qvarn.create('source', {'key': 1, 'value': 3}),

    # Handler should fail first time, but should pass the second time after retry.
    logger.info('RUN: 1')
    mocker.patch('time.time', return_value=1.0)
    process(qvarn, listeners, engine, raise_errors=False)
    assert get_resource_values(qvarn, 'mapped', ('_mr_key', '_mr_value')) == [
        (1, 1),
        (1, 3),
    ]
    assert len(qvarn.get_list('source/listeners/' + listenerid['source'] + '/notifications')) == 1

    # Handler should be retried only after 0.25 seconds, not sooner.
    logger.info('RUN: 2')
    mocker.patch('time.time', return_value=2.0)
    process(qvarn, listeners, engine, raise_errors=False)
    assert get_resource_values(qvarn, 'mapped', ('_mr_key', '_mr_value')) == [
        (1, 1),
        (1, 3),
    ]
    assert len(qvarn.get_list('source/listeners/' + listenerid['source'] + '/notifications')) == 1

    # Now enough time has passed, and handler should be retried.
    logger.info('RUN: 3')
    mocker.patch('time.time', return_value=3.0)
    process(qvarn, listeners, engine, raise_errors=False)
    assert get_resource_values(qvarn, 'mapped', ('_mr_key', '_mr_value')) == [
        (1, 1),
        (1, 2),
        (1, 3),
    ]
    assert len(qvarn.get_list('source/listeners/' + listenerid['source'] + '/notifications')) == 0


def test_reduce_handler_error(realqvarn, qvarn, mocker):
    realqvarn.add_resource_types(SCHEMA)

    config = {
        'mapped': {
            'source': {
                'type': 'map',
                'version': 1,
                'handler': item('key', 'value'),
            },
        },
        'reduced': {
            'mapped': {
                'type': 'reduce',
                'version': 1,
                'handler': mock.Mock(side_effect=[ValueError('fake error'), 42]),
            },
        },
    }

    engine = MapReduceEngine(qvarn, config)

    listeners = get_or_create_listeners(qvarn, 'test', config)
    listenerid = {x.source_resource_type: x.listener['id'] for x in listeners}

    qvarn.create('source', {'key': 1, 'value': 1}),
    qvarn.create('source', {'key': 1, 'value': 2}),
    qvarn.create('source', {'key': 1, 'value': 3}),

    # Handler should fail first time, but should pass the second time after retry.
    mocker.patch('time.time', return_value=1.0)
    process(qvarn, listeners, engine, raise_errors=False)
    assert get_resource_values(qvarn, 'reduced', ('_mr_key', '_mr_value')) == []
    assert len(qvarn.get_list('source/listeners/' + listenerid['source'] + '/notifications')) == 0
    assert len(qvarn.get_list('mapped/listeners/' + listenerid['mapped'] + '/notifications')) == 3

    # Handler should be retried only after 0.25 seconds, not sooner.
    mocker.patch('time.time', return_value=1.1)
    process(qvarn, listeners, engine, raise_errors=False)
    assert get_resource_values(qvarn, 'reduced', ('_mr_key', '_mr_value')) == []
    assert len(qvarn.get_list('source/listeners/' + listenerid['source'] + '/notifications')) == 0
    assert len(qvarn.get_list('mapped/listeners/' + listenerid['mapped'] + '/notifications')) == 3

    # Now enough time has passed, and handler should be retried.
    mocker.patch('time.time', return_value=2.0)
    process(qvarn, listeners, engine, raise_errors=False)
    assert get_resource_values(qvarn, 'reduced', ('_mr_key', '_mr_value')) == [
        (1, 42),
    ]
    assert len(qvarn.get_list('source/listeners/' + listenerid['source'] + '/notifications')) == 0
    assert len(qvarn.get_list('mapped/listeners/' + listenerid['mapped'] + '/notifications')) == 0


def test_map_outputs_dict_value(realqvarn, qvarn):
    realqvarn.add_resource_types({
        'data': {
            'path': '/data',
            'type': 'data',
            'versions': [
                {
                    'version': 'v1',
                    'prototype': {
                        'type': '',
                        'id': '',
                        'revision': '',
                        'foo': 0,
                        'bar': 0,
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
                        'result': 0,
                    },
                },
            ],
        },
    })

    def handler(resource):
        return None, {
            'result': resource['foo'] * resource['bar'],
        }

    config = {
        'data__map': {
            'data': {
                'type': 'map',
                'version': 1,
                'handler': handler,
            },
        },
    }

    listeners = get_or_create_listeners(qvarn, 'test', config)

    data = qvarn.create('data', {'foo': 4, 'bar': 4})

    process(qvarn, listeners, config)

    mapped = qvarn.get_list('data__map')
    assert len(mapped) == 1
    mapped = qvarn.get_multiple('data__map', mapped)
    mapped = {r['_mr_source_id']: r for r in mapped}
    assert cleaned(mapped[data['id']]) == {
        'type': 'data__map',
        '_mr_key': None,
        '_mr_value': None,
        '_mr_source_id': data['id'],
        '_mr_source_type': 'data',
        '_mr_version': 1,
        '_mr_deleted': 0,
        'result': 16,
    }


@pytest.mark.skip("multiple sources are not supported")
def test_single_source_multiple_targets(realqvarn, qvarn):
    realqvarn.add_resource_types({
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
                        'key': '',
                        'value': 0,
                    },
                },
            ],
        },
        'map1': {
            'path': '/map1',
            'type': 'map1',
            'versions': [
                {
                    'version': 'v1',
                    'prototype': {
                        'id': '',
                        'type': '',
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
        'map2': {
            'path': '/map2',
            'type': 'map2',
            'versions': [
                {
                    'version': 'v1',
                    'prototype': {
                        'id': '',
                        'type': '',
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
        'reduce1': {
            'path': '/reduce1',
            'type': 'reduce1',
            'versions': [
                {
                    'version': 'v1',
                    'prototype': {
                        'id': '',
                        'type': '',
                        'revision': '',
                        '_mr_key': '',
                        '_mr_value': 0,
                        '_mr_version': 0,
                        '_mr_timestamp': 0,
                    },
                },
            ],
        },
        'reduce2': {
            'path': '/reduce2',
            'type': 'reduce2',
            'versions': [
                {
                    'version': 'v1',
                    'prototype': {
                        'id': '',
                        'type': '',
                        'revision': '',
                        '_mr_key': '',
                        '_mr_value': 0,
                        '_mr_version': 0,
                        '_mr_timestamp': 0,
                    },
                },
            ],
        },
    })

    sum_reducer = mr_func()(lambda context, resources: sum(
        resource['_mr_value']
        for resource in context.qvarn.get_multiple(context.source_resource_type, resources)
    ))

    mul_reducer = mr_func()(lambda context, resources: reduce(mul, (
        resource['_mr_value']
        for resource in context.qvarn.get_multiple(context.source_resource_type, resources)
    )))

    config = {
        'map1': {
            'source': {
                'type': 'map',
                'version': 1,
                'handler': item('key', 'value'),
            },
        },
        'map2': {
            'source': {
                'type': 'map',
                'version': 1,
                'handler': item('key', 'value'),
            },
        },
        'reduce1': {
            'map1': {
                'type': 'reduce',
                'version': 1,
                'handler': sum_reducer(),
            },
            'map2': {
                'type': 'reduce',
                'version': 1,
                'handler': sum_reducer(),
            },
        },
        'reduce2': {
            'map1': {
                'type': 'reduce',
                'version': 1,
                'handler': mul_reducer(),
            },
            'map2': {
                'type': 'reduce',
                'version': 1,
                'handler': mul_reducer(),
            },
        },
    }

    listeners = get_or_create_listeners(qvarn, 'test', config)

    qvarn.create('source', {'key': '1', 'value': 2}),
    qvarn.create('source', {'key': '1', 'value': 4}),

    process(qvarn, listeners, config)

    assert get_resource_values(qvarn, 'map1', ('_mr_source_type', '_mr_key', '_mr_value')) == [
        ('source', '1', 2),
        ('source', '1', 4),
    ]
    assert get_resource_values(qvarn, 'map2', ('_mr_source_type', '_mr_key', '_mr_value')) == [
        ('source', '1', 2),
        ('source', '1', 4),
    ]

    assert get_resource_values(qvarn, 'reduce1', ('_mr_key', '_mr_value')) == [
        ('1', 6),
    ]
    assert get_resource_values(qvarn, 'reduce2', ('_mr_key', '_mr_value')) == [
        ('1', 8),
    ]
