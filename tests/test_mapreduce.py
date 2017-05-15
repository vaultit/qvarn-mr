from qvarnmr.func import join, items, values
from qvarnmr.worker import process
from qvarnmr.testing.utils import cleaned


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

    orgs_listener = qvarn.create('orgs/listeners', {
        'notify_of_new': True,
        'listen_on_all': True,
    })
    reports_listener = qvarn.create('reports/listeners', {
        'notify_of_new': True,
        'listen_on_all': True,
    })

    listeners = [
        ('orgs', orgs_listener),
        ('reports', reports_listener),
    ]

    org = qvarn.create('orgs', {'names': ['Orgtra']})
    reports = [
        qvarn.create('reports', {'org': org['id'], 'generated_timestamp': '1'}),
        qvarn.create('reports', {'org': org['id'], 'generated_timestamp': '2'}),
    ]

    process(qvarn, listeners, {
        'company_reports__map': [
            {
                'source': 'orgs',
                'type': 'map',
                'map': items('id'),
            },
            {
                'source': 'reports',
                'type': 'map',
                'map': items('org'),
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
    })

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

    reports_listener = qvarn.create('reports/listeners', {
        'notify_of_new': True,
        'listen_on_all': True,
    })

    listeners = [
        ('reports', reports_listener),
    ]

    org = qvarn.create('orgs', {'names': ['Orgtra']})
    reports = [
        qvarn.create('reports', {'org': org['id']}),
        qvarn.create('reports', {'org': org['id']}),
        qvarn.create('reports', {'org': org['id']}),
    ]

    process(qvarn, listeners, {
        'reports_counts__map': [
            {
                'source': 'reports',
                'type': 'map',
                'map': items('org'),
            },
        ],
        'reports_counts': [
            {
                'source': 'reports_counts__map',
                'type': 'reduce',
                'reduce': len,
            },
        ]
    })

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

    reports_listener = qvarn.create('data/listeners', {
        'notify_of_new': True,
        'listen_on_all': True,
    })

    listeners = [
        ('data', reports_listener),
    ]

    config = {
        'data_mapped': [
            {
                'source': 'data',
                'type': 'map',
                'map': items('key', 'value'),
            },
        ],
        'data_reduced': [
            {
                'source': 'data_mapped',
                'type': 'reduce',
                'map': values(),
                'reduce': sum,
            },
        ],
    }

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
