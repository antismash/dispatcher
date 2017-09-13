"""Dispatcher command line handling"""
import argparse
import os
from envparse import Env
from .app import StandaloneApplication
from .core import (
    dispatch,
    init_vars,
    teardown_containers,
    RunConfig,
)
from .database import DatabaseConfig, init_db, close_db
from .docker import init_docker, close_docker
from .log import setup_logging


def main():
    """Main CLI handling"""

    env = Env(
        # dispatcher name for management
        ASD_NAME=dict(cast=str, default=os.environ.get('HOSTNAME', 'dispatcher')),
        # Redis database
        ASD_DB=dict(cast=str, default='redis://localhost:6379/0'),
        # Redis queue
        ASD_QUEUE=dict(cast=str, default='jobs:queued'),
        # Working directory
        ASD_WORKDIR=dict(cast=str, default=os.path.join(os.getcwd(), 'upload')),
        # Directory to keep status files in
        ASD_STATUSDIR=dict(cast=str, default='/tmp/antismash_status'),
        # Job timeout in seconds, default 1 day (86400 s)
        ASD_TIMEOUT=dict(cast=int, default=86400),
        # CPUs to allocate
        ASD_CPUS=dict(cast=int, default=1),
        # Maximum jobs to run in parallel
        ASD_MAX_JOBS=dict(cast=int, default=5),
        # ClusterBlast database dir
        ASD_CLUSTERBLAST_DIR=dict(cast=str, default='/data/databases/clusterblast'),
        # PFAM database dir
        ASD_PFAM_DIR=dict(cast=str, default='/data/databases/pfam'),
        # uid/gid for running the container
        ASD_UID_STRING=dict(cast=str, default='{}:{}'.format(os.getuid(), os.getgid())),
    )

    parser = argparse.ArgumentParser(description='Dispatch antiSMASH containers')

    parser.add_argument('--database', dest='db',
                        default=env('ASD_DB'),
                        help="URI of the database containing the job queue (default: %(default)s).")
    parser.add_argument('-q', '--queue', dest='queue',
                        default=env('ASD_QUEUE'),
                        help="Name of the job queue (default: %(default)s).")
    parser.add_argument('-w', '--workdir', dest='workdir',
                        default=env('ASD_WORKDIR'),
                        help="Path to working directory containing the uploaded sequences (default: %(default)s).")
    parser.add_argument('-s', '--satusdir', dest='statusdir',
                        default=env('ASD_STATUSDIR'),
                        help="Path to directory containing the status files (default: %(default)s).")
    parser.add_argument('-n', '--name', dest='name',
                        default=env('ASD_NAME'),
                        help="Dispatcher name for management and status tracking (default: %(default)s).")
    parser.add_argument('-m', '--max-jobs', dest='max_jobs',
                        default=env('ASD_MAX_JOBS'), type=int,
                        help="Maximum number of antiSMASH jobs to run in parallel (default: %(default)s).")
    parser.add_argument('-c', '--cpus', dest='cpus',
                        default=env('ASD_CPUS'), type=int,
                        help="CPUs used per antiSMASH job (default: %(default)s).")
    parser.add_argument('-t', '--timeout', dest="timeout",
                        default=env('ASD_TIMEOUT'), type=int,
                        help="Timeout in seconds, (default: %(default)s).")
    parser.add_argument('-d', '--debug', dest='debug',
                        action='store_true', default=False,
                        help="Run antiSMASH in debug mode")
    parser.add_argument('--clusterblast-dir', dest='clusterblast_dir',
                        default=env('ASD_CLUSTERBLAST_DIR'),
                        help="ClusterBlast database directory (default: %(default)s).")
    parser.add_argument('--pfam-dir', dest='pfam_dir',
                        default=env('ASD_PFAM_DIR'),
                        help="PFAM database directory (default: %(default)s).")
    parser.add_argument('--uid-string', dest='uid_string',
                        default=env('ASD_UID_STRING'),
                        help="User ID the container should run as (default: %(default)s)")

    args = parser.parse_args()
    setup_logging()

    app = StandaloneApplication()

    db_conf = DatabaseConfig.from_argparse(args)
    app['db_conf'] = db_conf

    run_conf = RunConfig.from_argarse(args)
    app['run_conf'] = run_conf

    app.on_startup.append(init_db)
    app.on_startup.append(init_docker)
    app.on_startup.append(init_vars)

    # The order here is important
    app.on_cleanup.append(teardown_containers)
    app.on_cleanup.append(close_docker)
    app.on_cleanup.append(close_db)

    for i in range(args.max_jobs):
        app.tasks.append(dispatch)

    app.run()


if __name__ == "__main__":
    main()

