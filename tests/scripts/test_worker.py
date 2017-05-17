from qvarnmr.scripts import worker
from qvarnmr.func import item, value


def test_resync(pretender, qvarn, mocker, config):
    mocker.patch('qvarnmr.scripts.worker.set_config')
    mocker.patch('qvarnmr.scripts.worker.setup_qvarn_client', return_value=qvarn.client)

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

    mocker.patch('qvarnmr.testing.config', config, create=True)

    # Create notifications.
    worker.main(['qvarnmr.testing.config', '-c', 'qvarnmr.cfg'])

    # Create several resources.
    resources = [
        qvarn.create('data', {'key': 1, 'value': 1}),
        qvarn.create('data', {'key': 1, 'value': 2}),
        qvarn.create('data', {'key': 1, 'value': 3}),
    ]
    worker.main(['qvarnmr.testing.config', '-c', 'qvarnmr.cfg'])
    reduced = qvarn.get_list('data_reduced')
    reduced = qvarn.get('data_reduced', reduced[0])
    assert reduced['_mr_value'] == 6 and reduced['_mr_key'] == 1

    # Update some resources.
    qvarn.update('data', resources[0]['id'], {'key': 1, 'value': 2})
    qvarn.update('data', resources[2]['id'], {'key': 1, 'value': 5})
    worker.main(['qvarnmr.testing.config', '-c', 'qvarnmr.cfg'])
    reduced = qvarn.get_list('data_reduced')
    reduced = qvarn.get('data_reduced', reduced[0])
    assert reduced['_mr_value'] == 9 and reduced['_mr_key'] == 1

    # Delete some resources.
    qvarn.delete('data', resources[2]['id'])
    worker.main(['qvarnmr.testing.config', '-c', 'qvarnmr.cfg'])
    reduced = qvarn.get_list('data_reduced')
    reduced = qvarn.get('data_reduced', reduced[0])
    assert reduced['_mr_value'] == 4 and reduced['_mr_key'] == 1
