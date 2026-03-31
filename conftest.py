import sys
import os
import pytest

sys.path.insert(0, os.path.dirname(__file__))


def load_kafka_env():
    env = {}
    if os.path.exists('.kafka_env'):
        with open('.kafka_env') as f:
            for line in f:
                key, val = line.strip().split('=', 1)
                env[key] = val
    return env


@pytest.fixture(scope='module')
def kafka_env():
    env = load_kafka_env()
    if not env:
        raise RuntimeError("Kör 'python run_local.py' i en annan terminal först!")
    return env