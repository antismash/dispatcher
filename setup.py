import os
import sys
from setuptools import setup
from setuptools.command.test import test as TestCommand

short_description = "Dispatcher for web-submitted antiSMASH jobs"
long_description = short_description
if os.path.exists('README.rst'):
    long_description = open('README.rst').read()

install_requires = [
    'aioredis',
    'aiohttp',
    'aiodocker',
    'aiosmtplib',
    'envparse',
    'aiofiles',
    'antismash_models',
]


tests_require = [
    'pytest',
    'coverage',
    'pytest-cov',
    'mockaioredis',
    'pytest-asyncio',
]


def read_version():
    for line in open(os.path.join('dispatcher', 'version.py'), 'r'):
        if line.startswith('__version__'):
            return line.split('=')[-1].strip().strip("'")


class PyTest(TestCommand):
    def finalize_options(self):
        TestCommand.finalize_options(self)
        self.test_args = []
        self.test_suite = True

    def run_tests(self):
        import pytest
        errcode = pytest.main(self.test_args)
        sys.exit(errcode)


setup(
    name='antismash-dispatcher',
    version=read_version(),
    author='Kai Blin',
    author_email='kblin@biosustain.dtu.dk',
    description=short_description,
    long_description=long_description,
    install_requires=install_requires,
    tests_require=tests_require,
    cmdclass={'test': PyTest},
    entry_points={
        'console_scripts': [
            'as-dispatch=dispatcher.__main__:main'
        ],
    },
    packages=['dispatcher'],
    url='https://github.com/antismash/dispatcher/',
    license='GNU Affero General Public License v3 or later (AGPLv3+)',
    classifiers=[
        'Programming Language :: Python',
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Science/Research',
        'Topic :: Scientific/Engineering :: Bio-Informatics',
        'License :: OSI Approved :: GNU Affero General Public License v3 or later (AGPLv3+)',
        'Operating System :: OS Independent',
    ],
    extras_require={
        'testing': tests_require,
    },
)

