
import pytest

import redis


@pytest.fixture
def redis_conn():
    return redis.Redis(host='localhost', port=6379, db=0)

