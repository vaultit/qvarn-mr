def get_or_create_listeners(qvarn, instance: str, config: dict):
    listeners = []

    for target_resource_type, handlers in config.items():
        for handler in handlers:
            if handler['source'] not in config:
                qvarnmr_listener = qvarn.search_one(
                    'qvarnmr_listeners',
                    instance=instance,
                    resource_type=handler['source'],
                    default=None,
                )

                if qvarnmr_listener is None:
                    listener = qvarn.create(handler['source'] + '/listeners', {
                        'notify_of_new': True,
                        'listen_on_all': True,
                    })
                    qvarn.create('qvarnmr_listeners', {
                        'instance': instance,
                        'resource_type': handler['source'],
                        'listener_id': listener['id'],
                    })
                else:
                    listener = qvarn.get(handler['source'] + '/listeners', qvarnmr_listener['listener_id'])

                listeners.append((handler['source'], listener))

    return listeners
