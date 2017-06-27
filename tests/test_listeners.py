from operator import itemgetter

from qvarnmr.listeners import get_or_create_listeners


def test_get_listeners(pretender, qvarn):
    pretender.add_resource_types({
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
    })

    config = {
        'data__map': {
            'data1': {'type': 'map'},
            'data2': {'type': 'map'},
        },
        'data__join': {
            'data__map': {'type': 'reduce'},
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

    assert sorted(listeners, key=itemgetter(0)) == [
        ('data1', data1_listener),
        ('data2', data2_listener),
        ('data__map', data__map_listener),
    ]

    assert len(qvarn.get_list('data1/listeners')) == 1
    assert len(qvarn.get_list('data2/listeners')) == 1
