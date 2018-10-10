from argparse import Namespace
import mockaioredis
import os
import pytest
from dispatcher.core import RunConfig


@pytest.fixture
def args():
    args = Namespace(
        configfile=os.path.join(os.path.dirname(__file__), 'test.toml'),
        cpus=1,
        debug=False,
        entrez_url=None,
        max_jobs=1,
        name='dave',
        priority_queue='vip:line',
        queue="boring:line",
        run_priority=True,
        timeout=300,
        workdir='/workdir',
        uid_string="123:456"
    )
    return args


@pytest.fixture
def conf(args):
    return RunConfig.from_argparse(args)


@pytest.fixture
def db():
    return mockaioredis.MockRedis()
