"""Core dispatcher logic"""
from aiodocker.exceptions import DockerError
import asyncio
from enum import Enum
import logging
from aioredis import RedisError
import os
import time

from .download import download
from .mail import send_job_mail, send_error_mail
from .models import Control, Job


class JobOutcome(Enum):
    SUCCESS = 'success'
    FAILURE = 'failure'
    TIMEOUT = 'timeout'


async def dispatch(app):
    """Run the dispatcher main process."""
    pool = app['engine']
    db = await pool.acquire()
    run_conf = app['run_conf']
    run_conf.up()
    while True:
        try:
            if run_conf.want_less_jobs():
                app.logger.debug("%s shutting down a task", run_conf.name)
                run_conf.down()
                break

            uid = await db.brpoplpush(run_conf.queue, '{}:queued'.format(run_conf.name), timeout=5)
            if uid is None:
                await asyncio.sleep(5)
                continue

            job_db_conn = await pool.acquire()
            job = Job(job_db_conn, uid)
            await job.fetch()

            job.dispatcher = run_conf.name
            job.state = 'queued'
            job.status = 'queued: {}'.format(run_conf.name)
            await job.commit()

            await run_container(job, db, app)
            pool.release(job_db_conn)

        except RedisError as exc:
            app.logger.error("Got redis error: %r", exc)
            raise SystemExit()
        except asyncio.CancelledError:
            break
        except Exception as exc:
            app.logger.error("Got unhandled exception %s: '%s'", type(exc), str(exc))
            raise SystemExit()
    pool.release(db)


async def run_container(job, db, app):
    """Run a docker container for the given job

    :param job: A Job object representing the job to run
    :param db: A Redis database connection from the pool
    :param app: The app object with all the central config values
    :return:
    """
    docker = app['docker']
    timeout_tasks = app['timeout_tasks']
    containers = app['containers']
    run_conf = app['run_conf']

    # TODO: put download jobs in a separate queue, handle them in a separate task?
    if job.download != '':
        await download(job, app)

    app.logger.debug("Dispatching job %s", job)
    job.state = 'running'
    job.status = 'running'
    await job.commit()

    await db.lrem('{}:queued'.format(run_conf.name), 1, job.job_id)
    await db.lpush('jobs:running', job.job_id)

    # start a container that will run forever
    container = await docker.containers.create(
        config={
            'Cmd': create_commandline(job, run_conf),
            'Image': run_conf.image,
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

    res, warnings, errors = await event
    if res == JobOutcome.SUCCESS:
        timeout.cancel()
        job.state = 'done'
        job.status = 'done'
    elif res == JobOutcome.FAILURE:
        timeout.cancel()
        job.state = 'failed'
        error_lines = '\n'.join(errors)
        job.status = 'failed: Job returned errors: \n{}'.format(error_lines)
        await send_error_mail(app, job, warnings, errors)
    else:
        task.cancel()
        job.state = 'failed'
        job.status = 'failed: Runtime exceeded'
        await send_error_mail(app, job, warnings, errors)

    del timeout_tasks[container._id]

    await job.commit()

    await db.lrem('jobs:running', 1, job.job_id)
    await db.lpush('jobs:completed', job.job_id)

    app.logger.debug('Done with %s', container._id[:8])

    await container.delete(force=True)
    del containers[container._id]

    await send_job_mail(app, job, warnings, errors)

    app.logger.debug('Finished job %s', job)


async def follow(container, job, event):
    """Follow a container log

    :param container: a DockerContainer to follow
    :param job: the Job object running on the container
    :param event: the Future the parent task uses to track this run
    """
    timestamp = 0
    warnings = []
    errors = []
    while True:
        try:
            log = await container.log(stderr=True, stdout=True, follow=True, since=timestamp)
            async for line in log:
                line = line.strip()

                if line.startswith('INFO'):
                    job.status = 'running: {}'.format(line[25:])
                    job.changed()
                    await job.commit()
                elif line.startswith('WARNING'):
                    warnings.append(line)
                elif line.startswith('ERROR'):
                    errors.append(line)

                if line.endswith('SUCCESS'):
                    event.set_result((JobOutcome.SUCCESS, warnings, errors))
                    return
                elif line.endswith('FAILED'):
                    event.set_result((JobOutcome.FAILURE, warnings, errors))
                    return
                timestamp = int(time.time())
        except asyncio.TimeoutError:
            # Docker is dumb and times out after 5 minutes, just retry
            pass
        except (DockerError, KeyboardInterrupt):
            # Most likely the container got killed in the meantime, just exit
            return


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
        '--outputfolder', job_folder,
        '--logfile', os.path.join(job_folder, '{}.log'.format(job.job_id))
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
        event.set_result((JobOutcome.TIMEOUT, [], ['Runtime exceeded']))
    except DockerError:
        pass


async def init_vars(app):
    """Initialise the internal variables used"""
    app['containers'] = {}
    app['timeout_tasks'] = {}


async def teardown_containers(app):
    """Tear down all remaining docker containers"""
    containers = app['containers']
    db = await app['engine'].acquire()

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


async def manage(app):
    """Run the dispatcher management process."""
    pool = app['engine']
    db = await pool.acquire()
    run_conf = app['run_conf']
    control = Control(db, run_conf.name, run_conf.max_jobs)
    await control.commit()
    while True:
        await control.fetch()

        if control.stop_scheduled:
            control.max_jobs = 0
            await control.commit()

        run_conf.max_jobs = control.max_jobs
        if run_conf.want_more_jobs():
            app.logger.debug("Starting an extra task")
            app.start_task(dispatch)

        await asyncio.sleep(10)

        if run_conf.running_jobs == 0:
            break

    control.running = False
    control.stop_scheduled = False
    control.status = 'shut down'
    await control.commit()
    pool.release(db)


class RunConfig:
    """Container for runtime-related configuration"""
    __slots__ = (
        'clusterblast_dir',
        'cpus',
        'debug',
        'entrez_url',
        'image',
        'max_jobs',
        'name',
        'pfam_dir',
        'queue',
        'timeout',
        'workdir',
        'uid_string',
        '_running_jobs',
    )

    def __init__(self, *args):
        """Initialise a RunConfig"""

        for i, arg in enumerate(args):
            self.__setattr__(RunConfig.__slots__[i], arg)

        # Unlikely to change, so special case this
        self.entrez_url = 'https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi'
        self._running_jobs = 0

    @property
    def running_jobs(self):
        return self._running_jobs

    def want_more_jobs(self):
        """Check if less than max_jobs are running"""
        return self.running_jobs < self.max_jobs

    def want_less_jobs(self):
        """Check if more than max_jobs are running"""
        return self.running_jobs > self.max_jobs

    def up(self):
        """Called when a dispatcher task starts up"""
        self._running_jobs += 1

    def down(self):
        """Called when a dispatcher task shuts down"""
        self._running_jobs -= 1

    @classmethod
    def from_argarse(cls, args):
        """Instantiate a RunConfig from an argparse.Namespace

        :param args: argparse.Namespace to read values from
        :return: RunConfig instance
        """
        arg_list = []
        for arg in RunConfig.__slots__:
            if arg.startswith('_'):
                continue
            if arg == 'entrez_url':
                arg_list.append(None)
                continue
            arg_list.append(args.__getattribute__(arg))

        return cls(*arg_list)

