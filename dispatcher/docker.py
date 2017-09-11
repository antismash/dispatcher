"""Docker functions"""

import aiodocker
import asyncio


async def init_docker(app):
    """Init the docker connection"""
    docker = aiodocker.Docker()
    app['docker'] = docker
    app['docker_subscriber'] = subscriber


async def close_docker(app):
    """Shut down the events subscriber and the docker connection"""
    docker = app['docker']
    try:
        await docker.close()
    except asyncio.TimeoutError:
        pass
