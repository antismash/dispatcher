"""Docker functions"""

import aiodocker
import asyncio


async def init_docker(app):
    """Init the docker connection"""
    docker = aiodocker.Docker()
    subscriber = docker.events.subscribe()
    app['docker'] = docker
    app['docker_subscriber'] = subscriber


async def close_docker(app):
    """Shut down the events subscriber and the docker connection"""
    docker = app['docker']
    try:
        await docker.events.stop()
    except asyncio.TimeoutError:
        pass
    await docker.close()

