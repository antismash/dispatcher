"""Core dispatcher logic"""
from aiodocker.exceptions import DockerError
import asyncio
from datetime import datetime
from enum import Enum
import logging
from aioredis import RedisError
import os
import time

from .download import download
from .models import Job


class JobOutcome(Enum):
    SUCCESS = 'success'
    FAILURE = 'failure'
    TIMEOUT = 'timeout'


async def dispatch(app):
    """Run the dispatcher main process."""
    db = app['engine']
    run_conf = app['run_conf']
    while True:
        try:
            uid = await db.brpoplpush(run_conf.queue, '{}:queued'.format(run_conf.name), timeout=5)
            if uid is None:
                await asyncio.sleep(5)
                continue

            job = Job(db, uid)
            await job.fetch()

            job.dispatcher = run_conf.name
            job.state = 'queued'
            job.status = 'queued: {}'.format(run_conf.name)
            await job.commit()

            await run_container(job, app)

        except RedisError as exc:
            app.logger.error("Got redis error: %r", exc)
            raise SystemExit()


async def run_container(job, app):
    """Run a docker container for the given job

    :param job: A Job object representing the job to run
    :param app: The app object with all the central config values
    :return:
    """
    docker = app['docker']
    timeout_tasks = app['timeout_tasks']
    containers = app['containers']
    run_conf = app['run_conf']
    db = app['engine']

    # TODO: put download jobs in a separate queue, handle them in a separate task?
    if job.download != '':
        await download(job, app)

    app.logger.debug("Dispatching job %s", job)
    job.state = 'running'
    job.status = 'running'
    job.last_changed = datetime.utcnow()
    await job.commit()

    await db.lrem('{}:queued'.format(run_conf.name), 1, job.job_id)
    await db.lpush('jobs:running', job.job_id)

    # start a container that will run forever
    container = await docker.containers.create(
        config={
            'Cmd': create_commandline(job, run_conf),
            'Image': 'antismash/standalone-lite:4.0.2',
            'HostConfig': create_host_config(job, run_conf),
            'User': run_conf.uid_string,
        }
    )
    await container.start()
    app.logger.debug("Started %s", container._id[:8])
    containers[container._id] = (container, job)

    event = asyncio.Future(loop=app.loop)

    task = asyncio.ensure_future(follow(container, job, event))

    def timeout_handler():
        asyncio.ensure_future(cancel(container, event), loop=app.loop)

    timeout = app.loop.call_later(run_conf.timeout, timeout_handler)
    timeout_tasks[container._id] = timeout

    res = await event
    if res == JobOutcome.SUCCESS:
        timeout.cancel()
        job.state = 'done'
        job.status = 'done'
    elif res == JobOutcome.FAILURE:
        timeout.cancel()
        job.state = 'failed'
        # TODO: Grab the error message
        job.status = 'failed: INSERT ERROR MESSAGE'
    else:
        task.cancel()
        job.state = 'failed'
        job.status = 'failed: Runtime exceeded'

    del timeout_tasks[container._id]

    await job.commit()

    await db.lrem('jobs:running', 1, job.job_id)
    await db.lpush('jobs:complete', job.job_id)

    app.logger.debug('Done with %s', container._id[:8])

    await container.delete(force=True)
    del containers[container._id]

    app.logger.debug('Finished job %s', job)


async def follow(container, job, event):
    """Follow a container log

    :param container: a DockerContainer to follow
    :param job: the Job object running on the container
    :param event: the Future the parent task uses to track this run
    """
    timestamp = 0
    while True:
        log = await container.log(stderr=True, stdout=True, follow=True, since=timestamp)
        try:
            async for line in log:
                line = line.strip()

                if line.startswith('INFO'):
                    job.status = 'running: {}'.format(line[25:])
                    await job.commit()

                if line.endswith('SUCCESS'):
                    event.set_result(JobOutcome.SUCCESS)
                    return
                elif line.endswith('FAILED'):
                    event.set_result(JobOutcome.FAILURE)
                    return
                timestamp = int(time.time())
        except asyncio.TimeoutError:
            # Docker is dumb and times out after 5 minutes, just retry
            pass


def create_commandline(job, conf):
    """Create the command line to run an antiSMASH job

    :param job: Job object representing the job to run
    :return: A list of strings with the command line args
    """
    job_folder = os.path.join(os.sep, 'data', 'antismash', 'upload', job.job_id)

    args = [
        job.filename,
        '--cpus', str(conf.cpus),
        '--taxon', job.taxon,
        '--statusfile', os.path.join(conf.statusdir, job.job_id),
        '--outputfolder', job_folder,
        '--logfile', os.path.join(job_folder, '{}.log'.format(job.job_id))
    ]

    if job.minimal:
        args.append('--minimal')

    if conf.debug:
        args.append('--debug')
    else:
        args.append('--verbose')

    return args


def create_host_config(job, conf):
    """Create the HostConfig dict required by the docker API

    :param conf: A RunConfig instance with all runtime-related info
    :return: A dictionary representing a Docker API HostConfig
    """
    binds = [
        "{}:/databases/clusterblast:ro".format(conf.clusterblast_dir),
        "{}:/databases/pfam:ro".format(conf.pfam_dir),
        "{}:/data/antismash/upload".format(conf.workdir),
        "{}:/input:ro".format(os.path.join(conf.workdir, job.job_id)),
        "{0}:{0}".format(conf.statusdir),
    ]

    host_config = dict(Binds=binds)
    return host_config


async def cancel(container, event):
    """Kill the container once the timeout has expired

    :param container: Container object to kill
    :param event: Future tracking this container run
    """
    logging.debug("Timeout expired, killing container %s", container._id[:8])

    try:
        await container.kill()
        event.set_result(JobOutcome.TIMEOUT)
    except DockerError:
        pass


async def init_vars(app):
    """Initialise the internal variables used"""
    app['containers'] = {}
    app['timeout_tasks'] = {}


async def teardown_containers(app):
    """Tear down all remaining docker containers"""
    containers = app['containers']
    db = app['engine']

    while len(containers):
        app.logger.debug("cleaning containers")
        _, (container, job) = containers.popitem()
        try:
            await container.delete(force=True)
        except DockerError:
            pass

        if job.state not in ('done', 'failed'):
            job.state = 'failed'
            job.status = 'failed: Dispatcher killed'
            await job.commit()

        await db.lrem('jobs:running', 1, job.job_id)
        await db.lpush('jobs:manual', job.job_id)


class RunConfig:
    """Container for runtime-related configuration"""
    __slots__ = (
        'clusterblast_dir',
        'cpus',
        'debug',
        'entrez_url',
        'max_jobs',
        'name',
        'pfam_dir',
        'queue',
        'statusdir',
        'timeout',
        'workdir',
        'uid_string',
    )

    def __init__(self,
                 clusterblast_dir,
                 cpus,
                 debug,
                 max_jobs,
                 name,
                 pfam_dir,
                 queue,
                 statusdir,
                 timeout,
                 workdir,
                 uid_string):
        """Initialise a RunConfig"""
        self.clusterblast_dir = clusterblast_dir
        self.cpus = cpus
        self.debug = debug
        self.entrez_url = 'https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi'
        self.max_jobs = max_jobs
        self.name = name
        self.pfam_dir = pfam_dir
        self.queue = queue
        self.statusdir = statusdir
        self.timeout = timeout
        self.workdir = workdir
        self.uid_string = uid_string

    @classmethod
    def from_argarse(cls, args):
        """Instantiate a RunConfig from an argparse.Namespace

        :param args: argparse.Namespace to read values from
        :return: RunConfig instance
        """
        return cls(args.clusterblast_dir,
                   args.cpus,
                   args.debug,
                   args.max_jobs,
                   args.name,
                   args.pfam_dir,
                   args.queue,
                   args.statusdir,
                   args.timeout,
                   args.workdir,
                   args.uid_string)

