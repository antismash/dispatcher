"""Test antiSMASH command line generation"""
from antismash_models import AsyncJob as Job

from dispatcher.cmdline import create_commandline


def test_create_commandline6_minimal(conf, db):
    job = Job(db, 'bacteria-fake')
    job.jobtype = 'antismash6'
    job.filename = 'fake.gbk'
    job.minimal = True

    expected = [
        'fake.gbk',
        '--cpus', '1',
        '--taxon', 'bacteria',
        '--output-dir', '/data/antismash/upload/bacteria-fake',
        '--logfile', '/data/antismash/upload/bacteria-fake/bacteria-fake.log',
        '--debug',
        '--minimal'
    ]

    cmdline = create_commandline(job, conf)
    assert cmdline == expected


def test_create_commandline6_minimal_gff3(conf, db):
    job = Job(db, 'bacteria-fake')
    job.jobtype = 'antismash6'
    job.filename = 'fake.fa'
    job.gff3 = 'fake.gff'
    job.minimal = True

    expected = [
        'fake.fa',
        '--cpus', '1',
        '--taxon', 'bacteria',
        '--output-dir', '/data/antismash/upload/bacteria-fake',
        '--logfile', '/data/antismash/upload/bacteria-fake/bacteria-fake.log',
        '--debug',
        '--genefinding-gff3', '/input/fake.gff',
        '--minimal'
    ]

    cmdline = create_commandline(job, conf)
    assert cmdline == expected


def test_create_commandline6_all_options(conf, db):
    job = Job(db, 'bacteria-fake')
    job.jobtype = 'antismash6'
    job.filename = 'fake.gbk'
    job.asf = True
    job.clusterhmmer = True
    job.pfam2go = True
    job.clusterblast = True
    job.knownclusterblast = True
    job.subclusterblast = True
    job.genefinding = 'none'
    job.cassis = True

    expected = [
        'fake.gbk',
        '--cpus', '1',
        '--taxon', 'bacteria',
        '--output-dir', '/data/antismash/upload/bacteria-fake',
        '--logfile', '/data/antismash/upload/bacteria-fake/bacteria-fake.log',
        '--debug',
        '--asf',
        '--clusterhmmer',
        '--pfam2go',
        '--cb-general',
        '--cb-knownclusters',
        '--cb-subclusters',
        '--genefinding-tool', 'none',
        '--cassis',
    ]

    cmdline = create_commandline(job, conf)
    assert cmdline == expected


def test_create_commandline6_pfam2go_adds_clusterhmmer(conf, db):
    job = Job(db, 'bacteria-fake')
    job.jobtype = 'antismash6'
    job.filename = 'fake.gbk'
    job.pfam2go = True

    expected = [
        'fake.gbk',
        '--cpus', '1',
        '--taxon', 'bacteria',
        '--output-dir', '/data/antismash/upload/bacteria-fake',
        '--logfile', '/data/antismash/upload/bacteria-fake/bacteria-fake.log',
        '--debug',
        '--clusterhmmer',
        '--pfam2go',
        '--genefinding-tool', 'none',
    ]

    cmdline = create_commandline(job, conf)
    assert cmdline == expected


def test_create_commandline6_cassis_override(conf, db):
    conf.run_cassis = False

    job = Job(db, 'fungi-fake')
    job.jobtype = 'antismash6'
    job.filename = 'fake.gbk'
    job.cassis = True

    expected = [
        'fake.gbk',
        '--cpus', '1',
        '--taxon', 'fungi',
        '--output-dir', '/data/antismash/upload/fungi-fake',
        '--logfile', '/data/antismash/upload/fungi-fake/fungi-fake.log',
        '--debug',
        '--genefinding-tool', 'none',
    ]

    cmdline = create_commandline(job, conf)
    assert cmdline == expected
