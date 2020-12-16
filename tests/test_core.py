"""Tests for the core functions"""
from antismash_models import AsyncJob as Job

from dispatcher import core


def test_run_config(args):
    conf = core.RunConfig.from_argparse(args)
    assert conf.uid_string == args.uid_string

    assert conf.max_jobs == 1
    assert conf.running_jobs == 0
    assert conf.want_more_jobs()
    assert not conf.want_less_jobs()
    conf.up()
    assert conf.running_jobs == 1
    assert not conf.want_more_jobs()
    assert not conf.want_less_jobs()
    conf.up()
    assert conf.running_jobs == 2
    assert not conf.want_more_jobs()
    assert conf.want_less_jobs()
    conf.down()
    assert conf.running_jobs == 1


def test_create_host_config(conf, db):
    job = Job(db, 'bacteria-fake')
    job.jobtype = 'antismash6'

    expected = {
        "Binds": [
            "/as6/clusterblast:/databases/clusterblast:ro",
            "/as6/clustercompare:/databases/clustercompare:ro",
            "/as6/pfam:/databases/pfam:ro",
            "/as6/resfam:/databases/resfam:ro",
            "/as6/tigrfam:/databases/tigrfam:ro",
            "/workdir:/data/antismash/upload",
            "/workdir/bacteria-fake/input:/input:ro"
        ]
    }

    ret = core.create_host_config(job, conf)
    assert ret == expected
