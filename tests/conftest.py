import datetime

import pytest
import requests_mock
import dateutil.parser
from freezegun.api import FakeDatetime, FakeDate, FrozenDateTimeFactory, convert_to_timezone_naive

import qvarnmr.config
from qvarnmr.testing.realqvarn import RealQvarn

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
def realqvarn(request, mock_requests, mocker):
    return RealQvarn(mock_requests, QVARN_BASE_URL)


@pytest.fixture
def qvarn(realqvarn):
    return realqvarn.qvarn


@pytest.fixture
def config():
    qvarnmr.config.set_config(CONFIG)
    return qvarnmr.config.get_config()


@pytest.fixture
def freezetime(request, mocker):
    def unfreeze():
        FakeDate.dates_to_freeze.pop()
        FakeDate.tz_offsets.pop()

        FakeDatetime.times_to_freeze.pop()
        FakeDatetime.tz_offsets.pop()

    def freeze(time_to_freeze_str, tz_offset=0):
        if isinstance(time_to_freeze_str, datetime.datetime):
            time_to_freeze = time_to_freeze_str
        elif isinstance(time_to_freeze_str, datetime.date):
            time_to_freeze = datetime.datetime.combine(time_to_freeze_str, datetime.time())
        else:
            time_to_freeze = dateutil.parser.parse(time_to_freeze_str)

        time_to_freeze = convert_to_timezone_naive(time_to_freeze)
        time_to_freeze = FrozenDateTimeFactory(time_to_freeze)

        FakeDate.dates_to_freeze.append(time_to_freeze)
        FakeDate.tz_offsets.append(tz_offset)

        FakeDatetime.times_to_freeze.append(time_to_freeze)
        FakeDatetime.tz_offsets.append(tz_offset)

        mocker.patch('datetime.date', FakeDate)
        mocker.patch('datetime.datetime', FakeDatetime)

        request.addfinalizer(unfreeze)

    return freeze
