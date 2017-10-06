import pytest

from qvarnmr.validation import validate_handlers
from qvarnmr.exceptions import HandlerValidationError


def handler():
    pass


def test_different_types():
    with pytest.raises(HandlerValidationError) as e:
        validate_handlers({
            'map': {
                'source1': {
                    'type': 'map',
                    'version': 1,
                    'handler': handler,
                },
                'source2': {
                    'type': 'reduce',
                    'version': 1,
                    'handler': handler,
                },
            },
        })

    assert str(e.value) == (
        "Handler configuration error: map: all handlers of a single target must have same type, "
        "but there is more than one type used."
    )


def test_single_reduce_source():
    with pytest.raises(HandlerValidationError) as e:
        validate_handlers({
            'map': {
                'source': {
                    'type': 'map',
                    'version': 1,
                    'handler': handler,
                },
            },
            'reduce': {
                'map': {
                    'type': 'reduce',
                    'version': 1,
                    'handler': handler,
                },
                'source': {
                    'type': 'reduce',
                    'version': 1,
                    'handler': handler,
                },
            },
        })

    assert str(e.value) == (
        "Handler configuration error: reduce: currently only one handler is supported for reduce "
        "target, but 2 sources found."
    )


def test_unknown_fields():
    with pytest.raises(HandlerValidationError) as e:
        validate_handlers({
            'map': {
                'source': {
                    'type': 'map',
                    'version': 1,
                    'handler': handler,
                    'foo': 'bar',
                },
            },
        })

    assert str(e.value) == (
        "Handler configuration error: map <- source: unknown handler fields: foo."
    )


def test_missing_fields():
    with pytest.raises(HandlerValidationError) as e:
        validate_handlers({
            'map': {
                'source': {
                    'type': 'map',
                    'version': 1,
                },
            },
        })

    assert str(e.value) == (
        "Handler configuration error: map <- source: missing required handler fields: handler."
    )


def test_unknown_handler_type():
    with pytest.raises(HandlerValidationError) as e:
        validate_handlers({
            'map': {
                'source': {
                    'type': 'foo',
                    'version': 1,
                    'handler': handler,
                },
            },
        })

    assert str(e.value) == (
        "Handler configuration error: map <- source: handler type must be 'map' or 'reduce', but "
        "'foo' was given."
    )


def test_reduce_source_must_by_a_target():
    with pytest.raises(HandlerValidationError) as e:
        validate_handlers({
            'reduce': {
                'source': {
                    'type': 'reduce',
                    'version': 1,
                    'handler': handler,
                },
            },
        })

    assert str(e.value) == (
        "Handler configuration error: reduce <- source: source resource (source) for reduce "
        "target (reduce) must be defined as map target resource."
    )


def test_reduce_source_must_by_a_map_target():
    with pytest.raises(HandlerValidationError) as e:
        validate_handlers({
            'map': {
                'source': {
                    'type': 'map',
                    'version': 1,
                    'handler': handler,
                },
            },
            'reduce1': {
                'map': {
                    'type': 'reduce',
                    'version': 1,
                    'handler': handler,
                },
            },
            'reduce2': {
                'reduce1': {
                    'type': 'reduce',
                    'version': 1,
                    'handler': handler,
                },
            },
        })

    assert str(e.value) == (
        "Handler configuration error: reduce2 <- reduce1: source resource for (reduce1) reduce "
        "target (reduce2) must be defined as map target resource."
    )
