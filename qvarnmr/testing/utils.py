from itertools import count

from qvarnmr.handlers import get_handlers
from qvarnmr.processor import get_changes, process_changes


def cleaned(resource):
    return {k: v for k, v in resource.items() if k not in ('id', 'revision')}


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


def get_resource_values(qvarn, target, field):
    result = []
    resources = qvarn.get_list(target)
    for r in qvarn.get_multiple(target, resources):
        if isinstance(field, tuple):
            result.append(tuple(r[x] for x in field))
        else:
            result.append(r[field])
    return sorted(result)


def update_resource(qvarn, resource_type, resource_id, **kwargs):
    resource = qvarn.get(resource_type, resource_id)
    resource.update(kwargs)
    qvarn.update(resource_type, resource_id, resource)


def process(qvarn, listeners, config, limit=10):
    counter = count()
    changes_processed = 1
    mappers, reducers = get_handlers(config)
    while changes_processed > 0:
        changes = get_changes(qvarn, listeners)
        changes_processed = process_changes(qvarn, config, changes, mappers, reducers)
        assert next(counter) < limit
