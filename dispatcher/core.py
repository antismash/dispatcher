"""Core dispatcher logic"""
from aiodocker.exceptions import DockerError
from antismash_models import AsyncControl as Control, AsyncJob as Job
import asyncio
from datetime import datetime, timedelta
from enum import Enum
import logging
from aioredis import RedisError
import os
import time
import toml

from .cmdline import create_commandline
from .errors import InvalidJobType
from .mail import send_job_mail, send_error_mail
from .version import version_sync


class JobOutcome(Enum):
    SUCCESS = 'success'
    FAILURE = 'failure'
    TIMEOUT = 'timeout'


WORKER_MAX_JOBS = 2
WORKER_MAX_AGE = timedelta(days=1)


async def dispatch(app):
    """Run the dispatcher main process."""
    db = app['engine']
    run_conf = app['run_conf']
    run_conf.up()
    MY_QUEUE = '{}:queued'.format(run_conf.name)
    counter = 0
    startup_timestamp = datetime.utcnow()
    while True:
        try:
            if run_conf.want_less_jobs():
                app.logger.debug("%s shutting down a task", run_conf.name)
                run_conf.down()
                break
            if counter >= WORKER_MAX_JOBS:
                app.logger.info("%s: Max job count reached for task, shutting down", run_conf.name)
                run_conf.down()
                break
            if datetime.utcnow() - startup_timestamp > WORKER_MAX_AGE:
                app.logger.info("%s: Max worker age reached, shutting down", run_conf.name)
                run_conf.down()
                break

            uid = None

            if run_conf.run_priority:
                uid = await db.rpoplpush(run_conf.priority_queue, MY_QUEUE)
            if uid is None:
                uid = await db.rpoplpush(run_conf.queue, MY_QUEUE)
            if uid is None:
                await asyncio.sleep(5)
                continue

            counter += 1
            job = Job(db, uid)
            try:
                await job.fetch()
            except ValueError:
                app.logger.info('Failed to fetch job %s', uid)
                continue

            job.dispatcher = run_conf.name
            job.state = 'queued'
            job.status = 'queued: {}'.format(run_conf.name)
            await job.commit()

            await run_container(job, db, app)

        except RedisError as exc:
            app.logger.error("Got redis error: %r", exc)
            raise SystemExit()
        except asyncio.CancelledError:
            break
        except Exception as exc:
            app.logger.error("Got unhandled exception %s: '%s'", type(exc), str(exc))
            raise SystemExit()


async def run_container(job, db, app):
    """Run a docker container for the given job

    :param job: A Job object representing the job to run
    :param db: A Redis database connection
    :param app: The app object with all the central config values
    :return:
    """
    docker = app['docker']
    timeout_tasks = app['timeout_tasks']
    containers = app['containers']
    run_conf = app['run_conf']

    app.logger.debug("Dispatching job %s", job)
    job.state = 'running'
    job.status = 'running'
    await job.commit()

    await db.lrem('{}:queued'.format(run_conf.name), 1, job.job_id)
    await db.lpush('jobs:running', job.job_id)

    try:
        cmdline = create_commandline(job, run_conf)
    except InvalidJobType as err:
        app.logger.debug("Got invalid job type %s", str(err))
        job.state = 'failed'
        job.status = 'failed: Invalid job type'
        await job.commit()

        await db.lrem('jobs:running', 1, job.job_id)
        await db.lpush('jobs:completed', job.job_id)

        await update_stats(db, job)
        await send_error_mail(app, job, [], [], [])
        return

    config = {
        'Cmd': cmdline,
        'Image': run_conf.jobtype_config[job.jobtype]['image'],
        'HostConfig': create_host_config(job, run_conf),
        'User': run_conf.uid_string,
    }

    app.logger.debug("Starting container using %s", config)

    # start a container that will run forever
    container = await docker.containers.create(config=config)
    await container.start()
    app.logger.debug("Started %s", container._id[:8])
    containers[container._id] = (container, job)

    event = asyncio.Future(loop=app.loop)

    task = asyncio.ensure_future(follow(container, job, event, logger=app.logger))

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

        backtrace = [l.strip() for l in await container.log(stderr=True, tail=50)]
        await send_error_mail(app, job, warnings, errors, backtrace)

        if not errors:
            errors = backtrace

        error_lines = '\n'.join(errors)
        job.status = 'failed: Job returned errors: \n{}'.format(error_lines)

    else:
        task.cancel()
        job.state = 'failed'
        job.status = 'failed: Runtime exceeded'
        await send_error_mail(app, job, warnings, errors, [])

    del timeout_tasks[container._id]

    await job.commit()

    await db.lrem('jobs:running', 1, job.job_id)
    await db.lpush('jobs:completed', job.job_id)

    await update_stats(db, job)

    app.logger.debug('Done with %s', container._id[:8])

    try:
        await container.delete(force=True)
        del containers[container._id]
    except (asyncio.TimeoutError, DockerError):
        # This is awkward, let's just keep the container in the list and our
        # on-shutdown cleanup should take care of it.
        pass

    await send_job_mail(app, job, warnings, errors)

    app.logger.debug('Finished job %s', job)


async def update_stats(db, job):
    """Update the statistics for a job.

    This is used to keep per month/week/day stats of job execution.

    :param db: A Redis database connection
    :param job: A Job object to collect stats for
    """
    timestamps = (
        job.last_changed.strftime("%Y-%m-%d"),  # daily stats
        job.last_changed.strftime("%Y-CW%U"),   # weekly stats
        job.last_changed.strftime("%Y"),        # yearly stats
    )
    for ts in timestamps:
        await db.hset("jobs:{timestamp}".format(timestamp=ts), job.job_id, job.state)


async def follow(container, job, event, logger):
    """Follow a container log

    :param container: a DockerContainer to follow
    :param job: the Job object running on the container
    :param event: the Future the parent task uses to track this run
    :param logger: A logger class for writing logs
    """
    timestamp = 0
    warnings = set()
    errors = set()

    def order_logs(logs):
        return sorted(list(logs))

    while True:
        try:
            new_timestamp = int(time.time())
            log = await container.log(stderr=True, stdout=True, since=timestamp)
            timestamp = new_timestamp
            for line in log:
                line = line.strip()

                if line.startswith('INFO'):
                    job.status = 'running: {}'.format(line[25:])
                    job.changed()
                    await job.commit()
                elif line.startswith('WARNING'):
                    warnings.add(line)
                elif line.startswith('ERROR'):
                    errors.add(line)

                if line.endswith('SUCCESS'):
                    event.set_result((JobOutcome.SUCCESS, order_logs(warnings), order_logs(errors)))
                    return
                elif line.endswith('FAILED'):
                    event.set_result((JobOutcome.FAILURE, order_logs(warnings), order_logs(errors)))
                    return
            await asyncio.sleep(5)
        except asyncio.TimeoutError:
            # Docker is dumb and times out after 5 minutes, just retry
            logger.debug("Got a TimeoutError talking to Docker, retrying")
        except (DockerError, KeyboardInterrupt):
            # Most likely the container got killed in the meantime, just exit
            logger.debug("Got DockerError or KeyboardInterrupt, aborting")
            return


def create_host_config(job, conf):
    """Create the HostConfig dict required by the docker API

    :param conf: A RunConfig instance with all runtime-related info
    :return: A dictionary representing a Docker API HostConfig
    """
    job_conf = conf.jobtype_config[job.jobtype]
    binds = [
        "{}:/databases/clusterblast:ro".format(job_conf['clusterblastdir']),
        "{}:/databases/pfam:ro".format(job_conf['pfamdir']),
        "{}:/databases/resfam:ro".format(job_conf['resfamdir']),
        "{}:/data/antismash/upload".format(conf.workdir),
        "{}:/input:ro".format(os.path.join(conf.workdir, job.job_id, 'input')),
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
    db = await app['engine']

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
    db = app['engine']
    run_conf = app['run_conf']
    control = Control(db, run_conf.name, run_conf.max_jobs)
    await control.commit()
    while True:
        await control.fetch()

        if control.stop_scheduled:
            control.max_jobs = 0
            await control.commit()

        if control.running_jobs != run_conf.running_jobs:
            control.running_jobs = run_conf.running_jobs
            await control.commit()

        run_conf.max_jobs = control.max_jobs
        if run_conf.want_more_jobs():
            app.logger.debug("Starting an extra task")
            app.start_task(dispatch)

        await asyncio.sleep(10)

        if run_conf.running_jobs == 0 and not run_conf.want_more_jobs():
            break

    await control.delete()


class RunConfig:
    """Container for runtime-related configuration"""
    __slots__ = (
        'configfile',
        'cpus',
        'debug',
        'entrez_url',
        'max_jobs',
        'name',
        'priority_queue',
        'queue',
        'run_cassis',
        'run_priority',
        'timeout',
        'workdir',
        'uid_string',
        '_jobtype_config',
        '_running_jobs',
    )

    def __init__(self, *args):
        """Initialise a RunConfig"""

        for i, arg in enumerate(args):
            self.__setattr__(RunConfig.__slots__[i], arg)

        # Unlikely to change, so special case this
        self.entrez_url = 'https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi'
        self._running_jobs = 0

        self._jobtype_config = toml.load(self.configfile)

    @property
    def running_jobs(self):
        return self._running_jobs

    @property
    def jobtype_config(self):
        return self._jobtype_config

    @property
    def version(self):
        return version_sync()

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
    def from_argparse(cls, args):
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

