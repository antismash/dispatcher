"""Core dispatcher logic"""
from aiodocker.exceptions import DockerError
import asyncio
from datetime import datetime
import logging
from aioredis import RedisError
import os

from .download import download
from .log import core_logger
from .models import Job
from .signal import Signal


class StandaloneApplication:
    """A standalone async application to run"""

    def __init__(self, *, logger=core_logger):
        """Initialize the application to run

        :param logger: The logger class to use
        """
        self.logger = logger

        self._on_startup = Signal(self)
        self._on_shutdown = Signal(self)
        self._on_cleanup = Signal(self)

        self._state = {}
        self._loop = None
        self.tasks = []

    def __getitem__(self, key):
        return self._state[key]

    def __setitem__(self, key, value):
        self._state[key] = value

    @property
    def on_startup(self):
        return self._on_startup

    @property
    def on_shutdown(self):
        return self._on_shutdown

    @property
    def on_cleanup(self):
        return self._on_cleanup

    async def startup(self):
        """Trigger the startup callbacks"""
        await self.on_startup.send(self)

    async def shutdown(self):
        """Trigger the shutdown callbacks

        Call this before calling cleanup()
        """
        await self.on_shutdown.send(self)

    async def cleanup(self):
        """Trigger the cleanup callbacks

        Calls this after calling shutdown()
        """
        await self.on_cleanup.send(self)

    @property
    def loop(self):
        return self._loop

    @loop.setter
    def loop(self, loop):
        if loop is None:
            loop = asyncio.get_event_loop()

        if self._loop is not None and self._loop is not loop:
            raise RuntimeError("Can't override event loop after init")

        self._loop = loop

    def run(self, loop=None):
        """Actually run the application

        :param loop: Custom event loop or None for default
        """
        if loop is None:
            loop = asyncio.get_event_loop()

        self.loop = loop

        loop.run_until_complete(self.startup())

        for task in self.tasks:
            loop.create_task(task(self))

        try:
            loop.run_forever()
        except (KeyboardInterrupt, SystemError):
            print("Attempting graceful shutdown, press Ctrl-C again to exit", flush=True)

            def shutdown_exception_handler(_loop, context):
                if "exception" not in context or not isinstance(context["exception"], asyncio.CancelledError):
                    _loop.default_exception_handler(context)
            loop.set_exception_handler(shutdown_exception_handler)

            available_tasks = asyncio.Task.all_tasks(loop=loop)
            tasks = asyncio.gather(*available_tasks, loop=loop, return_exceptions=True)
            tasks.add_done_callback(lambda _: loop.stop())
            tasks.cancel()

            while not tasks.done() and not loop.is_closed():
                loop.run_forever()
        finally:
            loop.run_until_complete(self.shutdown())
            loop.run_until_complete(self.cleanup())
            loop.close()


async def dispatch(app):
    """Run the dispatcher main process."""
    containers = app['containers']
    db = app['engine']
    run_conf = app['run_conf']
    while True:
        try:
            # TODO: Handle control interaction

            if len(containers) > run_conf.max_jobs:
                await asyncio.sleep(5)
                continue

            uid = await db.brpoplpush('jobs:queued', '{}:queued'.format(run_conf.name), timeout=5)
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
            'User': '1000:1000',
        }
    )
    await container.start()
    app.logger.debug("Started %s", container._id[:8])

    def timeout_handler():
        asyncio.ensure_future(cancel(app, container, job), loop=app.loop)

    timeout = app.loop.call_later(run_conf.timeout, timeout_handler)

    timeout_tasks[container._id] = timeout
    containers[container._id] = (container, job)


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


async def cleanup(app):
    """Watch for container die events and clean up"""
    subscriber = app['docker_subscriber']
    containers = app['containers']
    timeout_tasks = app['timeout_tasks']
    db = app['engine']
    while True:
        event = await subscriber.get()
        if event is None:
            continue

        if event['Action'] not in ('die', 'kill'):
            continue

        container_id = event['Actor']['ID']
        print("Got die event for container", container_id)
        if container_id not in containers:
            app.logger.debug("Got event for unknown container %s", container_id)
            continue

        container, job = containers.pop(container_id)

        if container_id in timeout_tasks:
            timeout = timeout_tasks.pop(container_id)
            timeout.cancel()
        else:
            app.logger.debug("No timeout task found for %s", container_id)

        info = await container.show()
        exit_code = info['State']['ExitCode']
        app.logger.debug("args; %r, return code %s", info['Args'], exit_code)

        if exit_code != 0:
            log = await container.log(stdout=True, stderr=True)
            job.state = 'failed'
            job.status = 'failed: {}'.format(log)
            app.logger.debug(log)
        else:
            job.state = 'done'
            job.status = 'done'

        await job.commit()

        await db.lrem('jobs:running', 1, job.job_id)
        await db.lpush('jobs:complete', job.job_id)

        await container.delete(force=True)


async def cancel(app, container, job):
    """Kill the container once the timeout has expired

    :param app: The app containing all shared constructs
    :param container: Container object to kill
    :param job: Job object for job runnign on container
    """
    logging.debug("Timeout expired, killing container %s", container._id[:8])

    try:
        await container.kill()
        job.state = 'failed'
        job.status = 'failed: Runtime exceeded'
        await job.commit()
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
        'statusdir',
        'timeout',
        'workdir',
    )

    def __init__(self,
                 clusterblast_dir,
                 cpus,
                 debug,
                 max_jobs,
                 name,
                 pfam_dir,
                 statusdir,
                 timeout,
                 workdir):
        """Initialise a RunConfig"""
        self.clusterblast_dir = clusterblast_dir
        self.cpus = cpus
        self.debug = debug
        self.entrez_url = 'https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi'
        self.max_jobs = max_jobs
        self.name = name
        self.pfam_dir = pfam_dir
        self.statusdir = statusdir
        self.timeout = timeout
        self.workdir = workdir

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
                   args.statusdir,
                   args.timeout,
                   args.workdir)

