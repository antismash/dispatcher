"""Command line handling"""
import os


def create_commandline(job, conf):
    """Create the command line to run an antiSMASH job

    :param job: Job object representing the job to run
    :param conf: RunConfig object with the runtime configuration
    :return: A list of strings with the command line args
    """
    job_folder = os.path.join(os.sep, 'data', 'antismash', 'upload', job.job_id)

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
