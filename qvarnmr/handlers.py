import importlib
from collections import defaultdict


def import_handlers_config(path: str):
    module_name, config_variable_name = path.rsplit('.', 1)
    module = importlib.import_module(module_name)
    return getattr(module, config_variable_name)


def get_handlers(config):
    mappers = defaultdict(list)
    reducers = defaultdict(list)
    for target_resource_type, handlers in config.items():
        for source_resource_type, handler in handlers.items():
            if handler['type'] == 'map':
                mappers[source_resource_type].append((target_resource_type, handler))
            elif handler['type'] == 'reduce':
                reducers[source_resource_type].append((target_resource_type, handler))
            else:
                raise ValueError((
                    'Unknown map/reduce type, it should be map or reduce, got: %r'
                ) % handler['type'])
    return mappers, reducers
