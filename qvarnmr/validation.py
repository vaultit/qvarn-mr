import time

from qvarnmr.exceptions import HandlerValidationError


def validate_handlers(config):
    # Sanity check, Python documentation says, that some machines does not have precise clock. So
    # here we check if this machine has enough precision.
    assert int(time.time() * 1e9) - int(time.time() * 1e9) < 0

    targets = {}
    for target_resource_type, sources in config.items():
        handler_types = set()
        for source_resource_type, handler in sources.items():
            handler_types.add(handler['type'])
        if len(handler_types) > 1:
            raise HandlerValidationError(
                "Handler configuration error: {target}: all handlers of a single target must have "
                "same type, but there is more than one type used.".format(
                    target=target_resource_type,
                ))
        targets[target_resource_type] = next(iter(handler_types))

        if targets[target_resource_type] == 'reduce' and len(sources) != 1:
            raise HandlerValidationError(
                "Handler configuration error: {target}: currently only one handler is supported "
                "for reduce target, but {n_sources} sources found.".format(
                    target=target_resource_type,
                    n_sources=len(sources),
                ))

    required_handler_fields = {
        'map': {'type', 'version', 'handler'},
        'reduce': {'type', 'version', 'handler'},
    }
    optional_handler_fields = {
        'map': set(),
        'reduce': {'map'},
    }
    for target_resource_type, sources in config.items():
        for source_resource_type, handler in sources.items():
            defined_handler_fields = set(handler.keys())

            _handler_type = handler.get('type', 'map')
            _handler_type = _handler_type if _handler_type in ('map', 'reduce') else 'map'
            required_fields = required_handler_fields[_handler_type]
            optional_fields = optional_handler_fields[_handler_type]

            unknown_handler_fields = defined_handler_fields - (required_fields | optional_fields)
            if unknown_handler_fields:
                raise HandlerValidationError(
                    "Handler configuration error: {target} <- {source}: unknown handler fields: "
                    "{unknown_fields}.".format(
                        target=target_resource_type,
                        source=source_resource_type,
                        unknown_fields=', '.join(sorted(unknown_handler_fields)),
                    ))

            missing_handler_fields = required_fields - defined_handler_fields
            if missing_handler_fields:
                raise HandlerValidationError(
                    "Handler configuration error: {target} <- {source}: missing required handler "
                    "fields: {missing_fields}.".format(
                        target=target_resource_type,
                        source=source_resource_type,
                        missing_fields=', '.join(sorted(missing_handler_fields)),
                    ))

            if handler['type'] not in ('map', 'reduce'):
                raise HandlerValidationError(
                    "Handler configuration error: {target} <- {source}: handler type must be "
                    "'map' or 'reduce', but {type!r} was given.".format(
                        target=target_resource_type,
                        source=source_resource_type,
                        type=handler['type'],
                    ))

            if handler['type'] == 'reduce' and source_resource_type not in targets:
                raise HandlerValidationError(
                    "Handler configuration error: {target} <- {source}: source resource "
                    "({source}) for reduce target ({target}) must be defined as map target "
                    "resource.".format(
                        target=target_resource_type,
                        source=source_resource_type,
                    ))

            if handler['type'] == 'reduce' and targets[source_resource_type] != 'map':
                raise HandlerValidationError(
                    "Handler configuration error: {target} <- {source}: source resource "
                    "for ({source}) reduce target ({target}) must be defined as map target "
                    "resource.".format(
                        target=target_resource_type,
                        source=source_resource_type,
                    ))
