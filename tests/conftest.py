from argparse import Namespace
import mockaioredis
import pytest
from dispatcher.core import RunConfig


@pytest.fixture
def args():
    args = Namespace(
        clusterblast_dir='/clusterblast',
        cpus=1,
        debug=False,
        entrez_url=None,
        image='fake/image:latest',
        max_jobs=1,
        name='dave',
        pfam_dir='/pfam',
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
