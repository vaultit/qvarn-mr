def test_notifications(qvarn):
    # Create new listener
    listener = qvarn.create('orgs/listeners', {
        'notify_of_new': True,
        'listen_on_all': True,
    })

    # Create new resource
    org = qvarn.create('orgs', {'names': ['Orgtra']})
    assert org['names'] == ['Orgtra']

    # Check listener notifications
    notifications = qvarn.get_list('orgs/listeners/' + listener['id'] + '/notifications')
    assert len(notifications) == 1

    # Get details about received notification
    notification = qvarn.get('orgs/listeners/' + listener['id'] + '/notifications', notifications[0])
    del notification['last_modified']
    assert notification == {
        'id': notification['id'],
        'type': 'notification',
        'revision': notification['revision'],
        'listener_id': listener['id'],
        'resource_change': 'created',
        'resource_id': org['id'],
        'resource_revision': org['revision'],
    }

    # Delete notification
    qvarn.delete('orgs/listeners/' + listener['id'] + '/notifications', notifications[0])

    # Check listener notifications again
    notifications = qvarn.get_list('orgs/listeners/' + listener['id'] + '/notifications')
    assert len(notifications) == 0

    # Update resource
    org = qvarn.update('orgs', org['id'], dict(org, names=['Orgtra 2']))

    # Check listener notification for an update
    notifications = qvarn.get_list('orgs/listeners/' + listener['id'] + '/notifications')
    assert len(notifications) == 1

    # Get details about received notification
    notification = qvarn.get('orgs/listeners/' + listener['id'] + '/notifications', notifications[0])
    del notification['last_modified']
    assert notification == {
        'id': notification['id'],
        'type': 'notification',
        'revision': notification['revision'],
        'listener_id': listener['id'],
        'resource_change': 'updated',
        'resource_id': org['id'],
        'resource_revision': org['revision'],
    }

    # Delete notification
    qvarn.delete('orgs/listeners/' + listener['id'] + '/notifications', notifications[0])

    # Update resource
    qvarn.delete('orgs', org['id'])

    # Check listener notification for deleted resource
    notifications = qvarn.get_list('orgs/listeners/' + listener['id'] + '/notifications')
    assert len(notifications) == 1

    # Get details about received notification
    notification = qvarn.get('orgs/listeners/' + listener['id'] + '/notifications', notifications[0])
    del notification['last_modified']
    assert notification == {
        'id': notification['id'],
        'type': 'notification',
        'revision': notification['revision'],
        'listener_id': listener['id'],
        'resource_change': 'deleted',
        'resource_id': org['id'],
        'resource_revision': None,
    }

    # Delete notification
    qvarn.delete('orgs/listeners/' + listener['id'] + '/notifications', notifications[0])
