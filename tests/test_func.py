from qvarnmr.func import item


def test_func_repr():
    assert repr(item('id', 'value')) == "item('id', 'value')"
