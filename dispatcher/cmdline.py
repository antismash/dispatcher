"""Command line handling"""
from antismash_models import AsyncJob
import os

from dispatcher.errors import InvalidJobType


def create_commandline(job, conf) -> list[str]:
    """Create the command line to run an antiSMASH job

    :param job: Job object representing the job to run
    :param conf: RunConfig object with the runtime configuration
    :return: A list of strings with the command line args
    """

    if job.jobtype == 'antismash6':
        return create_commandline_as6(job, conf)
    elif job.jobtype == 'antismash7':
        return create_commandline_as7(job, conf)

    raise InvalidJobType(job.jobtype)


def create_commandline_as6(job, conf) -> list[str]:
    """Create the command line to run antiSMASH 6 jobs

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
        '--limit', str(conf.limit),
    ]

    if job.gff3:
        args.extend(['--genefinding-gff3', os.path.join(os.sep, 'input', job.gff3)])

    # All config that should work for both minimal and regular jobs needs to go above this line
    if job.minimal:
        args.append('--minimal')
        return args

    if job.asf:
        args.append('--asf')

    if job.hmmdetection_strictness:
        args.extend(['--hmmdetection-strictness', job.hmmdetection_strictness])

    if job.clusterhmmer:
        args.append('--clusterhmmer')
    if job.pfam2go:
        if '--clusterhmmer' not in args:
            args.append('--clusterhmmer')
        args.append('--pfam2go')

    if job.tigrfam:
        args.append('--tigrfam')

    if job.clusterblast:
        args.append('--cb-general')
    if job.knownclusterblast:
        args.append('--cb-knownclusters')
    if job.subclusterblast:
        args.append('--cb-subclusters')

    if job.cc_mibig:
        args.append('--cc-mibig')

    if job.genefinding:
        args.extend(['--genefinding-tool', job.genefinding])
    else:
        args.extend(['--genefinding-tool', 'none'])

    if job.smcog_trees:
        args.append('--smcog-trees')

    if job.rre:
        args.append('--rre')
        if job.rre_minlength:
            args.extend(['--rre-minlength', str(job.rre_minlength)])
        if job.rre_cutoff:
            args.extend(['--rre-cutoff', str(job.rre_cutoff)])

    if job.sideloads:
        filenames = [os.path.join(os.sep, 'input', s) for s in job.sideloads]
        args.extend(['--sideload', ",".join(filenames)])

    if job.sideload_simple:
        args.extend(['--sideload-simple', job.sideload_simple])

    if job.cassis and conf.run_cassis:
        args.append('--cassis')

    return args


def create_commandline_as7(job, conf) -> list[str]:
    """Create the command line to run antiSMASH 7 jobs

    :param job: Job object representing the job to run
    :param conf: RunConfig object with the runtime configuration
    :return: A list of strings with the command line args
    """

    args = create_commandline_as6(job, conf)

    if job.tfbs:
        args.append('--tfbs')

    if job.ncbi_context:
        args.append('--html-ncbi-context')

    return args


def _get_job_folder(job: AsyncJob) -> str:
    """Get the folder to store the job into inside the container."""
    return os.path.join(os.sep, 'data', 'antismash', 'upload', job.job_id)
