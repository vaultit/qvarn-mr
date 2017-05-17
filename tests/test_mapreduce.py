from unittest import mock

import pytest

from qvarnmr.func import join, item, value, count
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
                        'org_id': '',
                        'report_id': '',
                    },
                },
            ],
        },
    })

    config = {
        'company_reports__map': [
            {
                'source': 'orgs',
                'type': 'map',
                'map': item('id'),
            },
            {
                'source': 'reports',
                'type': 'map',
                'map': item('org'),
            },
        ],
        'company_reports': [
            {
                'source': 'company_reports__map',
                'type': 'reduce',
                'reduce': join({
                    'org': {
                        'id': 'org_id',
                    },
                    'report': {
                        'id': 'report_id',
                    },
                }),
            },
        ]
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
    assert cleaned(mapped[0]) == {
        'type': 'company_reports__map',
        '_mr_key': org['id'],
        '_mr_value': None,
        '_mr_source_id': org['id'],
        '_mr_source_type': 'orgs',
    }
    assert cleaned(mapped[1]) == {
        'type': 'company_reports__map',
        '_mr_key': org['id'],
        '_mr_value': None,
        '_mr_source_id': reports[0]['id'],
        '_mr_source_type': 'reports',
    }
    assert cleaned(mapped[2]) == {
        'type': 'company_reports__map',
        '_mr_key': org['id'],
        '_mr_value': None,
        '_mr_source_id': reports[1]['id'],
        '_mr_source_type': 'reports',
    }

    reduced = qvarn.get_list('company_reports')
    assert len(reduced) == 1
    reduced = qvarn.get('company_reports', reduced[0])
    assert cleaned(reduced) == {
        'type': 'company_report',
        '_mr_key': org['id'],
        '_mr_value': None,
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
                    },
                },
            ],
        },
    })

    config = {
        'reports_counts__map': [
            {
                'source': 'reports',
                'type': 'map',
                'map': item('org'),
            },
        ],
        'reports_counts': [
            {
                'source': 'reports_counts__map',
                'type': 'reduce',
                'reduce': count,
            },
        ]
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
                    },
                },
            ],
        },
    })

    config = {
        'data_mapped': [
            {
                'source': 'data',
                'type': 'map',
                'map': item('key', 'value'),
            },
        ],
        'data_reduced': [
            {
                'source': 'data_mapped',
                'type': 'reduce',
                'map': value(),
                'reduce': sum,
            },
        ],
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


def test_map_handler_error(pretender, qvarn):
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
                    },
                },
            ],
        },
    })

    config = {
        'data_mapped': [
            {
                'source': 'data',
                'type': 'map',
                'map': item('key', 'value'),
            },
        ],
        'data_reduced': [
            {
                'source': 'data_mapped',
                'type': 'reduce',
                'map': mock.Mock(side_effect=ValueError('imitated reduce error')),
                'reduce': sum,
            },
        ],
    }

    listeners = get_or_create_listeners(qvarn, 'test', config)

    # Create several resources.
    qvarn.create('data', {'key': 1, 'value': 1}),
    qvarn.create('data', {'key': 1, 'value': 2}),
    qvarn.create('data', {'key': 1, 'value': 3}),

    # Try to process changes.
    with pytest.raises(ValueError) as error:
        process(qvarn, listeners, config)
    assert str(error.value) == "imitated reduce error"

    # Since reducer failed, all notifications should be left in queue.
    listeners = dict(get_or_create_listeners(qvarn, 'test', config))
    notifications = qvarn.get_list('data/listeners/' + listeners['data']['id'] + '/notifications')
    assert len(notifications) == 3
