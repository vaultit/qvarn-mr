def ref(resource, key):
    yield resource[key], None


def join(resources, qvarn, resource_type, mapping):
    result = {}
    for resource in qvarn.get_multiple(resource_type, resources):
        source = qvarn.get(resource['_mr_source_type'], resource['_mr_source_id'])
        for key, name in mapping.get(source['type'], {}).items():
            name = name or key
            result[name] = source[key]
    return result
