"""Download-related logic"""

import aiofiles
import aiohttp
import os

from .log import download_logger

# TODO: Move to a separate service

error_patterns = (
    b'Error reading from remote server',
    b'Bad gateway',
    b'Bad Gateway',
    b'Cannot process ID list',
    b'server is temporarily unable to service your request',
    b'Service unavailable',
    b'Server Error',
    b'ID list is empty',
    b'Resource temporarily unavailable',
    b'In it there are neither IDs nor accessions',
    b'Failed to retrieve sequence',
    b'Failed to understand id',
)


async def download(job, app):
    conf = app['run_conf']
    async with aiohttp.ClientSession(loop=app.loop) as session:

        params = {
            'tool': 'antiSMASH downloader',
            'retmode': 'text',
            'id': job.download,
        }

        if job.molecule_type == 'nucl':
            params['db'] = 'nucleotide'
            params['rettype'] = 'gbwithparts'
            file_ending = '.gbk'
        elif job.molecule_type == 'prot':
            params['db'] = 'protein'
            params['rettype'] = 'fasta'
            file_ending = '.fa'
        else:
            download_logger.error("Invalid molecule_type %r, ignoring download.", job.molecule_type)
            job.state = 'failed'
            job.status = 'failed: Invalid molecule type {}'.format(job.molecule_type)
            await job.commit()
            return False

        outdir = os.path.join(conf.workdir, job.job_id, 'input')
        os.makedirs(outdir, exist_ok=True)

        base_filename = job.download + file_ending
        outfile = os.path.join(outdir, base_filename)
        job.filename = base_filename

        job.state = 'downloading'
        job.status = 'downloading: Getting {} from NCBI'.format(job.download)
        await job.commit()

        try:
            async with session.get(conf.entrez_url, params=params) as response, aiofiles.open(outfile, 'wb') as fh:
                while True:
                    chunk = await response.content.read(4096)
                    if not chunk:
                        job.state = 'running'
                        job.status = 'running: Downloaded {}'.format(base_filename)
                        break
                    for pattern in error_patterns:
                        if pattern in chunk:
                            job.state = 'failed'
                            job.status = "Failed to download file with id {} from NCBI: {}".format(job.download, pattern)
                            break
                    await fh.write(chunk)
        except aiohttp.client_exceptions.ClientConnectorError as err:
            job.state = 'failed'
            job.status = "Failed to download file with id {} from NCBI: {}".format(job.download, err)

        await job.commit()
        return True
