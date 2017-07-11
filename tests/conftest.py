import pytest
import requests_mock

import qvarnmr.config
from qvarnmr.testing.pretenderqvarn import PretenderQvarn

QVARN_BASE_URL = 'https://qvarn-example.tld'

CONFIG = {
    'qvarn': {
        'verify_requests': 'false',
        'base_url': 'https://qvarn-example.tld',
        'client_id': 'test_client_id',
        'client_secret': 'verysecret',
        'scope': 'scope1,scope2,scope3',
    },
    'qvarnmr': {
        'instance': 'test',
    }
}


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


@pytest.fixture
def config():
    qvarnmr.config.set_config(CONFIG)
    return qvarnmr.config.get_config()
