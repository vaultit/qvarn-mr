from unittest import mock

from qvarnmr.func import join, item, count, mr_func
from qvarnmr.processor import process
from qvarnmr.testing.utils import cleaned
from qvarnmr.listeners import get_or_create_listeners


def test_mapreduce(pretender, qvarn):
    pretender.add_resource_types({
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

    reduced = qvarn.get_list('company_reports')
    assert len(reduced) == 1
    reduced = qvarn.get('company_reports', reduced[0])
    assert cleaned(reduced) == {
        'type': 'company_report',
        '_mr_key': org['id'],
        '_mr_value': None,
        '_mr_version': 1,
        'org_id': org['id'],
        'report_id': reports[1]['id'],
    }


def test_reduce_scalar_value(pretender, qvarn):
    pretender.add_resource_types({
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


def test_create_update_delete_flow(pretender, qvarn):
    pretender.add_resource_types({
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
                    },
                },
            ],
        },
    })

    @mr_func()
    def sum_values(context, resources):
        resources = context.qvarn.get_multiple(context.source_resource_type, resources)
        return sum(x['_mr_value'] for x in resources)

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
                'handler': sum_values(),
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
    reduced = qvarn.get_list('data_reduced')
    reduced = qvarn.get('data_reduced', reduced[0])
    assert reduced['_mr_value'] == 6 and reduced['_mr_key'] == 1

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


def test_reduce_handler_error(pretender, qvarn):
    pretender.add_resource_types({
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
                        '_mr_deleted': 0,
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
                'map': mock.Mock(side_effect=ValueError('imitated reduce error')),
            },
        },
    }

    listeners = get_or_create_listeners(qvarn, 'test', config)

    # Create several resources.
    qvarn.create('data', {'key': 1, 'value': 1}),
    qvarn.create('data', {'key': 1, 'value': 2}),
    qvarn.create('data', {'key': 1, 'value': 3}),

    # Try to process changes.
    process(qvarn, listeners, config)

    # Since reducer failed, all notifications should be left in queue.
    listeners = dict(get_or_create_listeners(qvarn, 'test', config))
    notifications = qvarn.get_list('data/listeners/' + listeners['data']['id'] + '/notifications')
    assert len(notifications) == 3


def test_map_outputs_dict_value(pretender, qvarn):
    pretender.add_resource_types({
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
