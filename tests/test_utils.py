from qvarnmr.func import item, count
from qvarnmr.utils import get_handler_identifier, chunks, is_empty


def test_get_handler_identifier():
    assert get_handler_identifier(count) == 'qvarnmr.func.count'
    assert get_handler_identifier(item('id')) == "qvarnmr.func.item('id')"
    assert get_handler_identifier(item('org')) == "qvarnmr.func.item('org')"


def test_chunks():
    assert list(map(list, chunks(2, [1, 2, 3, 4, 5]))) == [[1, 2], [3, 4], [5]]
    assert list(map(list, chunks(2, iter([1, 2, 3, 4, 5])))) == [[1, 2], [3, 4], [5]]
    assert list(map(list, chunks(2, [1, 2, 3, 4]))) == [[1, 2], [3, 4]]
    assert list(map(list, chunks(2, []))) == []


def test_is_empty():
    iterable, empty = is_empty(iter([1, 2, 3]))
    assert list(iterable) == [1, 2, 3]
    assert empty is False

    iterable, empty = is_empty(iter([]))
    assert list(iterable) == []
    assert empty is True
