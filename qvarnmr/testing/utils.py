def cleaned(resource):
    return {k: v for k, v in resource.items() if k not in ('id', 'revision')}
