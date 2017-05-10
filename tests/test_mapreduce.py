from qvarnmr.func import ref, join
from qvarnmr.worker import ContextVar, process
from qvarnmr.testing.utils import cleaned


def test_mapreduce(qvarn):
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
                'func': ref,
                'args': ['id'],
            },
            {
                'source': 'reports',
                'type': 'map',
                'func': ref,
                'args': ['org'],
            },
        ],
        'company_reports': [
            {
                'source': 'company_reports__map',
                'type': 'reduce',
                'func': join,
                'args': [ContextVar('qvarn'), ContextVar('source_resource_type'), {
                    'org': {
                        'id': 'org_id',
                    },
                    'report': {
                        'id': 'report_id',
                    },
                }],
            },
        ]
    })

    mapped = qvarn.get_list('company_reports__map')
    assert len(mapped) == 3
    mapped = qvarn.get_multiple('company_reports__map', mapped)
    assert cleaned(mapped[0]) == {
        'type': 'company_reports__map',
        '_mr_key': org['id'],
        '_mr_source_id': org['id'],
        '_mr_source_type': 'orgs',
    }
    assert cleaned(mapped[1]) == {
        'type': 'company_reports__map',
        '_mr_key': org['id'],
        '_mr_source_id': reports[0]['id'],
        '_mr_source_type': 'reports',
    }
    assert cleaned(mapped[2]) == {
        'type': 'company_reports__map',
        '_mr_key': org['id'],
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
