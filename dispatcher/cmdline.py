"""Command line handling"""
from antismash_models import AsyncJob
import os

from .errors import InvalidJobType

def create_commandline(job, conf):
    """Create the command line to run an antiSMASH job

    :param job: Job object representing the job to run
    :param conf: RunConfig object with the runtime configuration
    :return: A list of strings with the command line args
    """

    # For now, default to running antiSMASH 4 jobs if nothing is set
    if job.jobtype == 'antismash4' or job.jobtype is None:
        return create_commandline_as4(job, conf)
    elif job.jobtype == 'antismash5':
        return create_commandline_as5(job, conf)

    raise InvalidJobType(job.jobtype)


def create_commandline_as4(job, conf):
    """Create the command line to run antiSMASH 4 jobs

    :param job: Job object representing the job to run
    :param conf: RunConfig object with the runtime configuration
    :return: A list of strings with the command line args
    """

    job_folder = _get_job_folder(job)

    args = [
        job.filename,
        '--cpus', str(conf.cpus),
        '--taxon', job.taxon,
        '--outputfolder', job_folder,
        '--logfile', os.path.join(job_folder, '{}.log'.format(job.job_id)),
        '--input-type', job.molecule_type,
    ]

    if conf.debug:
        args.append('--debug')
    else:
        args.append('--verbose')

    if job.gff3:
        args.extend(['--gff3', os.path.join(os.sep, 'input', job.gff3)])

    # All config that should work for both minimal and regular jobs needs to go above this line
    if job.minimal:
        args.append('--minimal')
        return args

    if job.smcogs:
        args.append('--smcogs')
    if job.asf:
        args.append('--asf')
    if job.tta:
        args.append('--tta')
    if job.cassis:
        args.append('--cassis')
    if job.transatpks_da:
        args.append('--transatpks_da')

    if job.clusterblast:
        args.append('--clusterblast')
    if job.knownclusterblast:
        args.append('--knownclusterblast')
    if job.subclusterblast:
        args.append('--subclusterblast')

    if job.full_hmmer:
        args.append('--full-hmmer')
        args += ['--limit', '1000']
    if job.borderpredict:
        args.append('--borderpredict')

    if job.inclusive:
        args.append('--inclusive')
        args += [
            '--cf_cdsnr', str(job.cf_cdsnr),
            '--cf_npfams', str(job.cf_npfams),
            '--cf_threshold', str(job.cf_threshold)
        ]
        if '--limit' not in args:
            args += ['--limit', '1000']

    if job.all_orfs:
        args.append('--all_orfs')

    # TODO: mismatch between database name and parameter name, fix in websmash
    if job.genefinder:
        args += ['--genefinding', job.genefinder]

    return args


def create_commandline_as5(job, conf):
    """Create the command line to run antiSMASH 5 jobs

    :param job: Job object representing the job to run
    :param conf: RunConfig object with the runtime configuration
    :return: A list of strings with the command line args
    """

    job_folder = _get_job_folder(job)

    args = [
        job.filename,
        '--cpus', str(conf.cpus),
        '--taxon', job.taxon,
        '--output-dir', job_folder,
        '--logfile', os.path.join(job_folder, '{}.log'.format(job.job_id)),
        '--debug',  # TODO: read this from the config later
    ]

    if job.gff3:
        args.extend(['--genefinding-gff3', os.path.join(os.sep, 'input', job.gff3)])

    # All config that should work for both minimal and regular jobs needs to go above this line
    if job.minimal:
        args.append('--minimal')
        return args

    if job.asf:
        args.append('--asf')

    if job.clusterhmmer:
        args.append('--clusterhmmer')
    if job.pfam2go:
        if '--custerhmmer' not in args:
            args.append('--clusterhmmer')
        args.append('--pfam2go')

    if job.clusterblast:
        args.append('--cb-general')
    if job.knownclusterblast:
        args.append('--cb-knownclusters')
    if job.subclusterblast:
        args.append('--cb-subclusters')

    if job.genefinding:
        args.extend(['--genefinding-tool', job.genefinding])
    else:
        args.extend(['--genefinding-tool', 'none'])

    return args


def _get_job_folder(job: AsyncJob) -> str:
    """Get the folder to store the job into inside the container."""
    return os.path.join(os.sep, 'data', 'antismash', 'upload', job.job_id)
