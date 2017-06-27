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
        result.append(r[field])
    return result
