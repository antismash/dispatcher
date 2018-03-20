"""Dispatcher command line handling"""
import argparse
import os
from aiostandalone import StandaloneApplication
from envparse import Env
from .core import (
    dispatch,
    init_vars,
    manage,
    teardown_containers,
    RunConfig,
)
from .database import DatabaseConfig, init_db, close_db
from .docker import init_docker, close_docker
from .log import setup_logging, core_logger
from .mail import EmailConfig, init_mail, close_mail
from .version import version_sync


def main():
    """Main CLI handling"""

    env = Env(
        # dispatcher name for management
        ASD_NAME=dict(cast=str, default=os.environ.get('HOSTNAME', 'dispatcher')),
        # Redis database
        ASD_DB=dict(cast=str, default='redis://localhost:6379/0'),
        # Regular queue
        ASD_QUEUE=dict(cast=str, default='jobs:queued'),
        # Priority queue
        ASD_PRIORITY_QUEUE=dict(cast=str, default='jobs:priority'),
        # Should the priority queue be run?
        ASD_RUN_PRIORITY=dict(cast=bool, default=True),
        # Working directory
        ASD_WORKDIR=dict(cast=str, default=os.path.join(os.getcwd(), 'upload')),
        # Docker image to use
        ASD_IMAGE=dict(cast=str, default="antismash/standalone-lite:latest"),
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
        # email password
        ASD_EMAIL_PASSWORD=dict(cast=str, default=''),
        # email username
        ASD_EMAIL_USER=dict(cast=str, default=''),
        # encryption mechanism to use
        ASD_EMAIL_ENCRYPT=dict(cast=str, default='no'),
        # email host
        ASD_EMAIL_HOST=dict(cast=str, default=''),
        # email to use as sender
        ASD_EMAIL_FROM=dict(cast=str, default=''),
        # email to use for errors
        ASD_EMAIL_ERROR=dict(cast=str, default=''),
        # email to use for support requests
        ASD_EMAIL_SUPPORT=dict(cast=str, default=''),
        # tool name for email
        ASD_TOOL_NAME=dict(cast=str, default='antiSMASH'),
        # base URL to use in emails
        ASD_BASE_URL=dict(cast=str, default='https://antismash.secondarymetabolites.org'),
    )

    parser = argparse.ArgumentParser(description='Dispatch antiSMASH containers')

    parser.add_argument('--database', dest='db',
                        default=env('ASD_DB'),
                        help="URI of the database containing the job queue (default: %(default)s).")
    parser.add_argument('-q', '--queue', dest='queue',
                        default=env('ASD_QUEUE'),
                        help="Name of the job queue (default: %(default)s).")
    parser.add_argument('-p', '--priority-queue',
                        default=env('ASD_PRIORITY_QUEUE'),
                        help="Name of the priority queue (default: %(default)s).")
    parser.add_argument('--run-priority', dest='run_priority',
                        action='store_true',
                        help="Enable processing the priority queue (default: %(default)s).")
    parser.add_argument('--no-priority', dest='run_priority',
                        action='store_false',
                        help="Disable processing the priority queue.")
    parser.add_argument('-w', '--workdir', dest='workdir',
                        default=env('ASD_WORKDIR'),
                        help="Path to working directory containing the uploaded sequences (default: %(default)s).")
    parser.add_argument('-i', '--image', dest='image',
                        default=env('ASD_IMAGE'),
                        help="Docker image to run (default: %(default)s).")
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
    parser.add_argument('-V', '--version', action='version',
                        version=version_sync())
    parser.set_defaults(run_priority=env('ASD_RUN_PRIORITY'))

    args = parser.parse_args()
    setup_logging()

    app = StandaloneApplication(logger=core_logger)

    db_conf = DatabaseConfig.from_argparse(args)
    app['db_conf'] = db_conf

    run_conf = RunConfig.from_argparse(args)
    app['run_conf'] = run_conf

    mail_conf = EmailConfig.from_env(env)
    app['mail_conf'] = mail_conf

    app.on_startup.append(init_db)
    app.on_startup.append(init_docker)
    app.on_startup.append(init_mail)
    app.on_startup.append(init_vars)

    # The order here is important
    app.on_cleanup.append(teardown_containers)
    app.on_cleanup.append(close_mail)
    app.on_cleanup.append(close_docker)
    app.on_cleanup.append(close_db)

    for i in range(args.max_jobs):
        app.tasks.append(dispatch)
    app.main_task = manage

    app.run()


if __name__ == "__main__":
    main()

