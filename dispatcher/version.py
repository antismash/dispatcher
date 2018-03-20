import asyncio
import subprocess

__version__ = '0.1.0'

_GIT_VERSION = None


async def version():
    """Get the full version string."""
    version = __version__

    git_ver = await git_version()
    if git_ver:
        version = "{}-{}".format(version, git_ver)

    return version


def version_sync():
    """Get the full version string, synchronous version."""
    version = __version__

    git_ver = git_version_sync()
    if git_ver:
        version = "{}-{}".format(version, git_ver)

    return version


async def git_version():
    """Get the git version."""
    global _GIT_VERSION
    if _GIT_VERSION is None:
        proc = await asyncio.create_subprocess_exec('git', 'rev-parse', '--short', 'HEAD')

        stdout, stderr = await proc.communicate()
        if proc.returncode != 0:
            ver = ""
        else:
            ver = stdout.strip().decode('utf-8')
        _GIT_VERSION = ver

    return _GIT_VERSION


def git_version_sync():
    """Get the git version, synchronous version."""
    global _GIT_VERSION
    if _GIT_VERSION is None:
        proc = subprocess.Popen(['git', 'rev-parse', '--short', 'HEAD'],
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        stdout, stderr = proc.communicate()
        if proc.returncode != 0:
            ver = ""
        else:
            ver = stdout.strip().decode('utf-8')
        _GIT_VERSION = ver

    return _GIT_VERSION
