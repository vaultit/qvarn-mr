def get_or_create_listeners(qvarn, instance: str, config: dict):
    listeners = []

    for target_resource_type, handlers in config.items():
        for source_resource_type, handler in handlers.items():
            qvarnmr_listener = qvarn.search_one(
                'qvarnmr_listeners',
                instance=instance,
                resource_type=source_resource_type,
                default=None,
            )

            if qvarnmr_listener is None:
                listener = qvarn.create(source_resource_type + '/listeners', {
                    'notify_of_new': True,
                    'listen_on_all': True,
                })
                qvarn.create('qvarnmr_listeners', {
                    'instance': instance,
                    'resource_type': source_resource_type,
                    'listener_id': listener['id'],
                })
            else:
                listener = qvarn.get(source_resource_type + '/listeners',
                                     qvarnmr_listener['listener_id'])

            listeners.append((source_resource_type, listener))

    return listeners
