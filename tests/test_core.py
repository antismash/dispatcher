"""Tests for the core functions"""
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
