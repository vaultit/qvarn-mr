import pytest
import requests_mock

from qvarnmr.testing.pretenderqvarn import PretenderQvarn

QVARN_BASE_URL = 'https://qvarn-example.tld'


@pytest.yield_fixture
def mock_requests():
    with requests_mock.mock() as m:
        yield m


@pytest.fixture
def pretender(request, mock_requests, mocker):
    return PretenderQvarn(mock_requests, QVARN_BASE_URL)


@pytest.fixture
def qvarn(pretender):
    return pretender.qvarn
