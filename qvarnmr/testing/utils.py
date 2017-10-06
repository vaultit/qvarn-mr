from itertools import count

from qvarnmr.processor import get_changes, MapReduceEngine


def cleaned(resource):
    return {k: v for k, v in resource.items() if k not in ('id', 'revision', '_mr_timestamp')}


def get_mapped_data(qvarn, target, expect_n_resources=None):
    resources = qvarn.get_list(target)
    if expect_n_resources is not None:
        assert len(resources) == expect_n_resources, 'expected %d, got %d' % (expect_n_resources,
                                                                              len(resources))
    result = dict()
    for r in qvarn.get_multiple(target, resources):
        assert r['_mr_source_id'] not in result
        result[r['_mr_source_id']] = cleaned(r)
    return result


def get_reduced_data(qvarn, target, expect_n_resources=None):
    resources = qvarn.get_list(target)
    if expect_n_resources is not None:
        assert len(resources) == expect_n_resources, 'expected %d, got %d' % (expect_n_resources,
                                                                              len(resources))
    result = dict()
    for r in qvarn.get_multiple(target, resources):
        assert r['_mr_key'] not in result
        result[r['_mr_key']] = cleaned(r)
    return result


def get_resource_values(qvarn, target, field, sort=None):
    result = []
    resources = qvarn.get_list(target)
    for r in qvarn.get_multiple(target, resources):
        if isinstance(field, tuple):
            result.append(tuple(r[x] for x in field))
        else:
            result.append(r[field])
    return sorted(result, key=sort)


def update_resource(qvarn, _resource_type, *args, **kwargs):
    """Update resource content.

    Update resource by resource id:

        update_resource(qvarn, 'resource_type', resource_id)(value=42)

    Update resource by search query:

        update_resource(qvarn, 'resource_type', field=value)(value=42)

    Update resource assuming only one resource exists:

        update_resource(qvarn, 'resource_type')(value=42)

    """
    assert len(args) <= 1

    if len(args) == 0 and not kwargs:
        resources = qvarn.get_list(_resource_type)
        assert len(resources) == 1
        resource = qvarn.get(_resource_type, resources[0])

    elif len(args) == 1:
        resource = qvarn.get(_resource_type, args[0])

    else:
        resource = qvarn.search_one(_resource_type, **kwargs)

    def updater(**data):
        resource.update(data)
        return qvarn.update(_resource_type, resource['id'], resource)

    return updater


def process(qvarn, listeners, config_or_engine, limit=10, raise_errors=True):
    if isinstance(config_or_engine, MapReduceEngine):
        engine = config_or_engine
    else:
        engine = MapReduceEngine(qvarn, config_or_engine, raise_errors=raise_errors)
    counter = count()
    changes_processed = 1
    while changes_processed > 0:
        changes = get_changes(qvarn, listeners)
        changes_processed = engine.process_changes(changes)
        assert next(counter) < limit
