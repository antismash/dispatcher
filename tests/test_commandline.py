"""Test antiSMASH command line generation"""
from antismash_models import AsyncJob as Job

from dispatcher.cmdline import create_commandline


def test_create_commandline4_minimal(conf, db):
    job = Job(db, 'bacteria-fake')
    job.filename = 'fake.gbk'
    job.minimal = True

    expected = [
        'fake.gbk',
        '--cpus', '1',
        '--taxon', 'bacteria',
        '--outputfolder', '/data/antismash/upload/bacteria-fake',
        '--logfile', '/data/antismash/upload/bacteria-fake/bacteria-fake.log',
        '--input-type', 'nucl',
        '--verbose',
        '--minimal'
    ]

    cmdline = create_commandline(job, conf)
    assert cmdline == expected


def test_create_commandline4_minimal_debug(conf, db):
    job = Job(db, 'bacteria-fake')
    job.filename = 'fake.gbk'
    job.minimal = True
    conf.debug = True

    expected = [
        'fake.gbk',
        '--cpus', '1',
        '--taxon', 'bacteria',
        '--outputfolder', '/data/antismash/upload/bacteria-fake',
        '--logfile', '/data/antismash/upload/bacteria-fake/bacteria-fake.log',
        '--input-type', 'nucl',
        '--debug',
        '--minimal'
    ]

    cmdline = create_commandline(job, conf)
    assert cmdline == expected


def test_create_commandline4_inclusive(conf, db):
    job = Job(db, 'bacteria-fake')
    job.filename = 'fake.gbk'
    job.inclusive = True
    job.cf_cdsnr = 1
    job.cf_npfams = 2
    job.cf_threshold = 0.3

    expected = [
        'fake.gbk',
        '--cpus', '1',
        '--taxon', 'bacteria',
        '--outputfolder', '/data/antismash/upload/bacteria-fake',
        '--logfile', '/data/antismash/upload/bacteria-fake/bacteria-fake.log',
        '--input-type', 'nucl',
        '--verbose',
        '--inclusive',
        '--cf_cdsnr', '1',
        '--cf_npfams', '2',
        '--cf_threshold', '0.3',
        '--limit', '1000',
        '--genefinding', 'none',
    ]

    cmdline = create_commandline(job, conf)
    assert cmdline == expected


def test_create_commandline4_minimal_gff3(conf, db):
    job = Job(db, 'bacteria-fake')
    job.filename = 'fake.fa'
    job.minimal = True
    job.gff3 = 'fake.gff'

    expected = [
        'fake.fa',
        '--cpus', '1',
        '--taxon', 'bacteria',
        '--outputfolder', '/data/antismash/upload/bacteria-fake',
        '--logfile', '/data/antismash/upload/bacteria-fake/bacteria-fake.log',
        '--input-type', 'nucl',
        '--verbose',
        '--gff3', '/input/fake.gff',
        '--minimal'
    ]

    cmdline = create_commandline(job, conf)
    assert cmdline == expected


def test_create_commandline4_all_options(conf, db):
    job = Job(db, 'bacteria-fake')
    job.filename = 'fake.gbk'
    job.smcogs = True
    job.asf = True
    job.tta = True
    job.cassis = True
    job.transatpks_da = True
    job.clusterblast = True
    job.knownclusterblast = True
    job.subclusterblast = True
    job.full_hmmer = True
    job.borderpredict = True
    job.inclusive = True
    job.cf_cdsnr = 1
    job.cf_npfams = 2
    job.cf_threshold = 0.3
    job.all_orfs = True
    job.genefinder = 'prodigal'

    expected = [
        'fake.gbk',
        '--cpus', '1',
        '--taxon', 'bacteria',
        '--outputfolder', '/data/antismash/upload/bacteria-fake',
        '--logfile', '/data/antismash/upload/bacteria-fake/bacteria-fake.log',
        '--input-type', 'nucl',
        '--verbose',
        '--smcogs',
        '--asf',
        '--tta',
        '--cassis',
        '--transatpks_da',
        '--clusterblast',
        '--knownclusterblast',
        '--subclusterblast',
        '--full-hmmer',
        '--limit', '1000',
        '--borderpredict',
        '--inclusive',
        '--cf_cdsnr', '1',
        '--cf_npfams', '2',
        '--cf_threshold', '0.3',
        '--all_orfs',
        '--genefinding', 'prodigal'
    ]

    cmdline = create_commandline(job, conf)
    assert cmdline == expected
