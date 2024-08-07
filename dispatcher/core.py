"""Core dispatcher logic"""
import asyncio
from asyncio import Future
from asyncio.subprocess import Process
from datetime import datetime, timedelta
from enum import Enum
import os
import subprocess

from aiostandalone import StandaloneApplication
from antismash_models import AsyncControl as Control, AsyncJob as Job
from redis import RedisError
from redis.asyncio import Redis
import toml

from .cmdline import create_commandline
from .errors import InvalidJobType, JobDirMissing
from .mail import send_job_mail, send_error_mail
from .version import version_sync, git_version


class JobOutcome(Enum):
    SUCCESS = 'success'
    FAILURE = 'failure'
    TIMEOUT = 'timeout'
    INTERNAL_ERROR = "internal_error"


WORKER_MAX_JOBS = 2
WORKER_MAX_AGE = timedelta(days=1)
CONTROL_UPDATE_SLEEP = 10


async def dispatch(app: StandaloneApplication):
    """Run the dispatcher main process."""
    db = app['engine']
    run_conf = app['run_conf']
    run_conf.up()
    MY_QUEUE = f"{run_conf.name}:queued"
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
            job.trace.append(run_conf.name)
            job.state = 'queued'
            job.status = 'queued: {}'.format(run_conf.name)
            await job.commit()

            await run_container(job, db, app)

        except RedisError as exc:
            app.logger.error("Got redis error: %r", exc)
            raise SystemExit() from exc
        except asyncio.CancelledError:
            break
        except Exception as exc:
            app.logger.error("Got unhandled exception %s: '%s'", type(exc), str(exc))
            raise SystemExit() from exc


async def run_container(job: Job, db: Redis, app: StandaloneApplication):
    """Run a container for the given job

    :param job: A Job object representing the job to run
    :param db: A Redis database connection
    :param app: The app object with all the central config values
    :return:
    """
    containers = app['containers']
    run_conf = app['run_conf']
    MY_QUEUE = f"{run_conf.name}:queued"

    app.logger.debug("Dispatching job %s", job)
    job.state = 'running'
    job.status = 'running'
    await job.commit()

    await db.lrem(MY_QUEUE, 1, job.job_id)
    await db.lpush('jobs:running', job.job_id)

    try:
        as_cmdline = create_commandline(job, run_conf)
        cmdline = create_podman_command(job, run_conf, as_cmdline)
    except InvalidJobType as err:
        app.logger.debug("Got invalid job type %s", str(err))
        job.state = 'failed'
        job.status = 'failed: Invalid job type'
        await job.commit()

        await db.lrem('jobs:running', 1, job.job_id)
        await db.lpush('jobs:completed', job.job_id)

        await update_stats(db, job)
        await send_error_mail(app, job, [], [], [], job.status)
        return
    except JobDirMissing as err:
        app.logger.debug("Job input directory missing: %s", str(err))
        job.state = 'failed'
        job.status = 'failed: Job input directory missing, was it stale?'
        await job.commit()

        await db.lrem('jobs:running', 1, job.job_id)
        await db.lpush('jobs:completed', job.job_id)

        await update_stats(db, job)
        await send_error_mail(app, job, [], [], [], job.status)
        return

    app.logger.debug("Starting container using %s", cmdline)

    event: Future = asyncio.Future()

    proc: Process = await asyncio.create_subprocess_exec(*cmdline, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    containers[job.job_id] = job

    def timeout_handler():
        asyncio.ensure_future(cancel(app, event, job.job_id))

    timeout = app.loop.call_later(run_conf.timeout, timeout_handler)
    task = asyncio.ensure_future(follow(app, proc, job, event))

    res, warnings, errors, backtrace = await event
    app.logger.debug("process for %s returned: %s", job.job_id, res)
    if res == JobOutcome.SUCCESS:
        timeout.cancel()
        job.state = 'done'
        job.status = 'done'
        del containers[job.job_id]
    elif res == JobOutcome.FAILURE:
        timeout.cancel()
        job.state = 'failed'

        await send_error_mail(app, job, warnings, errors, backtrace, res.name)

        if not errors:
            errors = backtrace

        error_lines = '\n'.join(errors)
        job.status = 'failed: Job returned errors: \n{}'.format(error_lines)
        del containers[job.job_id]
    elif res == JobOutcome.INTERNAL_ERROR:
        timeout.cancel()
        del containers[job.job_id]

        job.state = "queued"
        job.status = "queued"

        await job.commit()
        await trigger_shutdown(app)

        await db.lrem("jobs:running", 1, job.job_id)
        await db.lpush(run_conf.priority_queue, job.job_id)

        await send_error_mail(app, job, warnings, errors, backtrace, res.name)
        app.logger.debug('Finished job %s with internal error', job)
        return
    else:
        task.cancel()
        job.state = 'failed'
        job.status = 'failed: Runtime exceeded'
        await send_error_mail(app, job, warnings, errors, [], res.name)

    await job.commit()

    await db.lrem('jobs:running', 1, job.job_id)
    await db.lpush('jobs:completed', job.job_id)

    await update_stats(db, job)

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


async def follow(app: StandaloneApplication, proc: Process, job: Job, event: Future):
    """Follow a container log

    :param app: app object
    :param proc: Process object of the running podman process
    :param job: the Job object running on the container
    :param event: the Future the parent task uses to track this run
    """
    warnings: list[str] = []
    errors: list[str] = []
    backtrace: list[str] = []

    app.logger.debug("Starting to follow job %s", job.job_id)

    assert proc.stdout

    data = await proc.stdout.readline()
    while data:
        line = data.decode("utf-8").strip()

        if line.startswith('INFO'):
            job.status = 'running: {}'.format(line[25:])
            job.changed()
            await job.commit()
        elif line.startswith('WARNING'):
            warnings.append(line)
        elif line.startswith('ERROR'):
            errors.append(line)

        backtrace.append(line)
        if len(backtrace) > 50:
            backtrace.pop(0)

        if line.endswith('SUCCESS'):
            event.set_result((JobOutcome.SUCCESS, warnings, errors, backtrace))
            app.logger.debug("Stop following %s, it's done.", job.job_id)
            return
        elif line.endswith('FAILED'):
            event.set_result((JobOutcome.FAILURE, warnings, errors, backtrace))
            app.logger.debug("Stop following %s, it failed.", job.job_id)
            return
        data = await proc.stdout.readline()
    await asyncio.sleep(1.0)
    app.logger.debug("After follow() loop fell through: exitcode %s, output %s", proc.returncode, backtrace)
    event.set_result((JobOutcome.INTERNAL_ERROR, [],
                     [f"podman returned {proc.returncode}"], backtrace))


def create_podman_command(job: Job, conf: "RunConfig", as_cmdline: list[str]) -> list[str]:
    """Create the podman command to launch the container


    :param job: An antiSMASH Job object of the job to run
    :param conf: A RunConfig instance with all runtime-related info
    :param as_cmdline: A list of all the parameters that should be passed to antiSMASH
    :return: A list of podman command line parameters
    """
    try:
        job_conf = conf.jobtype_config[job.jobtype]
    except KeyError:
        raise InvalidJobType(job.jobtype)

    jobdir = os.path.join(conf.workdir, job.job_id, 'input')   # type: ignore  # yes mypy, conf has a workdir
    if not os.path.exists(jobdir):
        raise JobDirMissing(jobdir)

    mounts = [
        f"{job_conf['databases']}:/databases:ro",
        f"{conf.workdir}:/data/antismash/upload",  # type: ignore  # yes mypy, conf has a workdir
        f"{jobdir}:/input:ro",
    ]

    cmdline = ["podman", "run", "--detach=false", "--cgroup-manager", "cgroupfs"]
    if not conf.keep:  # type: ignore
        cmdline.append("--rm")

    for mount in mounts:
        cmdline.extend(["--volume", mount])

    cmdline.extend(["--name", job.job_id])

    cmdline.append(f"{job_conf['image']}")

    cmdline.extend(as_cmdline)

    return cmdline


async def cancel(app: StandaloneApplication, event: Future, container_name: str):
    """Kill the container once the timeout has expired

    :param app: app object
    :param event: Future the parent task uses to track the job
    :param container_name: Container object to kill
    """
    app.logger.debug("Timeout expired, killing container %s", container_name)

    proc = await asyncio.create_subprocess_exec("podman", "kill", container_name, stdout=subprocess.DEVNULL)
    await proc.communicate()
    del app['containers'][container_name]
    event.set_result((JobOutcome.TIMEOUT, [], ["Runtime exceeded"], []))


async def init_vars(app):
    """Initialise the internal variables used"""
    app['containers'] = {}


async def teardown_containers(app):
    """Tear down all remaining containers"""
    containers = app['containers']
    db = await app['engine']

    while len(containers):
        app.logger.debug("cleaning containers")
        container_name, job = containers.popitem()

        proc = await asyncio.create_subprocess_exec("podman", "kill", container_name, stdout=subprocess.DEVNULL)
        await proc.communicate()

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
    version_hash = await git_version()
    control = Control(db, run_conf.name, run_conf.max_jobs, version_hash)
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

        await control.alive()
        await asyncio.sleep(CONTROL_UPDATE_SLEEP)

        if run_conf.running_jobs == 0 and not run_conf.want_more_jobs():
            break

    await control.delete()


async def trigger_shutdown(app: StandaloneApplication):
    """Trigger a dispatcher shutdown"""
    db = app['engine']
    run_conf = app['run_conf']
    version_hash = await git_version()
    control = Control(db, run_conf.name, run_conf.max_jobs, version_hash)
    await control.fetch()
    control.stop_scheduled = True
    await control.commit()
    await asyncio.sleep(CONTROL_UPDATE_SLEEP + 3)


class RunConfig:
    """Container for runtime-related configuration"""
    __slots__ = (
        'configfile',
        'cpus',
        'debug',
        'keep',
        'limit',
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
            arg_list.append(args.__getattribute__(arg))

        return cls(*arg_list)
