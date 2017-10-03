'Datatypes that hit the Redis DB'
from datetime import datetime


class Job:
    """An antiSMASH job as represented in the Redis DB"""
    VALID_TAXA = {'bacterial', 'fungal', 'plant'}

    PROPERTIES = [
        'genefinder',
        'molecule_type',
        'state',
    ]

    INTERNAL = [
        '_db',
        '_id',
        '_key',
        '_taxon',
    ]

    ATTRIBUTES = [
        'added',
        'all_orfs',
        'asf',
        'borderpredict',
        'cassis',
        'cf_cdsnr',
        'cf_npfams',
        'cf_threshold',
        'clusterblast',
        'clusterfinder',
        'dispatcher',
        'download',
        'email',
        'filename',
        'from_pos',
        'full_hmmer',
        'gff3',
        'inclusive',
        'jobtype',
        'knownclusterblast',
        'last_changed',
        'minimal',
        'seed',
        'smcogs',
        'status',
        'subclusterblast',
        'to_pos',
        'transatpks_da',
        'tta',
    ]

    __slots__ = ATTRIBUTES + list(map(lambda x: '_%s' % x, PROPERTIES)) + INTERNAL

    BOOL_ARGS = {
        'all_orfs',
        'asf',
        'borderpredict',
        'cassis',
        'full_hmmer',
        'clusterblast',
        'clusterfinder',
        'inclusive',
        'knownclusterblast',
        'minimal',
        'smcogs',
        'subclusterblast',
        'transatpks_da',
        'tta',
    }

    INT_ARGS = {
        'cf_cdsnr',
        'cf_npfams',
        'from_pos',
        'seed',
        'to_pos',
    }

    FLOAT_ARGS = {
        'cf_threshold',
    }

    DATE_ARGS = {
        'added',
        'last_changed',
    }

    VALID_STATES = {
        'created',
        'downloading',
        'validating',
        'queued',
        'running',
        'done',
        'failed'
    }

    def __init__(self, db, job_id):
        self._db = db
        self._id = job_id
        self._key = 'job:{}'.format(self._id)

        # taxon is the first element of the ID
        self._taxon = self._id.split('-')[0]

        # storage for properties
        self._state = 'created'
        self._molecule_type = 'nucl'
        self._genefinder = 'none'

        for attribute in self.ATTRIBUTES:
            setattr(self, attribute, None)

        # Regular attributes that differ from None
        self.status = 'pending'

    # Not really async, but follow the same API as the other properties
    @property
    def job_id(self):
        return self._id

    # No setter, job_id is a read-only property

    # Not really async, but follow same API as the other properties
    @property
    def taxon(self):
        return self._taxon

    # No setter, taxon is a read-only property

    @property
    def state(self):
        return self._state

    @state.setter
    def state(self, value):
        if value not in self.VALID_STATES:
            raise ValueError('Invalid state {}'.format(value))

        self._state = value
        self.changed()

    @property
    def molecule_type(self):
        return self._molecule_type

    @molecule_type.setter
    def molecule_type(self, value):
        if value not in {'nucl', 'prot'}:
            raise ValueError('Invalid molecule_type {}'.format(value))

        self._molecule_type = value

    @property
    def genefinder(self):
        return self._genefinder

    @genefinder.setter
    def genefinder(self, value):
        if value not in {'prodigal', 'prodigal-m', 'none'}:
            raise ValueError('Invalid genefinding method {}'.format(value))
        self._genefinder = value

    @staticmethod
    def is_valid_taxon(taxon: str) -> bool:
        """
        Check if taxon string is one of 'bacterial', 'fungal' or 'plant'
        """
        if taxon not in Job.VALID_TAXA:
            return False

        return True

    def changed(self):
        """Update the job's last changed timestamp"""
        self.last_changed = datetime.utcnow()

    def to_dict(self, extra_info=False):
        ret = {}

        args = self.PROPERTIES + self.ATTRIBUTES

        for arg in args:
            if getattr(self, arg) is not None:
                arg_val = getattr(self, arg)

                # aioredis can't handle bool or datetime types, int and float are fine
                if arg in self.BOOL_ARGS:
                    arg_val = str(arg_val)
                elif arg in self.DATE_ARGS:
                    arg_val = arg_val.strftime("%Y-%m-%d %H:%M:%S.%f")

                ret[arg] = arg_val

        if extra_info:
            ret['job_id'] = self.job_id
            ret['taxon'] = self.taxon

        return ret

    def __str__(self):
        return "Job(id: {}, state: {})".format(self._id, self.state)

    async def fetch(self):
        args = self.PROPERTIES + self.ATTRIBUTES

        job_exists = await self._db.exists(self._key)
        if job_exists == 0:
            raise ValueError("No job with ID {} in database, can't fetch".\
                             format(self.job_id))

        values = await self._db.hmget(self._key, *args)

        for i, arg in enumerate(args):
            val = values[i]

            if val is None:
                continue

            if arg in self.BOOL_ARGS:
                val = (val != 'False')
            elif arg in self.INT_ARGS:
                val = int(val)
            elif arg in self.FLOAT_ARGS:
                val = float(val)
            elif arg in self.DATE_ARGS:
                val = datetime.strptime(val, "%Y-%m-%d %H:%M:%S.%f")

            setattr(self, arg, val)

    async def commit(self):
        return await self._db.hmset_dict(self._key, self.to_dict())


class Control:
    """Dispatcher management object"""

    ATTRIBUTES = (
        'name',
        'running',
        'stop_scheduled',
        'status',
        'max_jobs',
    )

    INTERNAL = (
        '_db',
        '_key',
    )

    __slots__ = ATTRIBUTES + INTERNAL

    BOOL_ARGS = {
        'running',
        'stop_scheduled'
    }

    INT_ARGS = {
        'max_jobs',
    }

    def __init__(self, db, name, max_jobs):
        self.name = name
        self._db = db
        self._key = "control:{}".format(self.name)
        self.stop_scheduled = False
        self.running = True
        self.status = 'running'
        self.max_jobs = max_jobs

    async def fetch(self):
        """Fetch object from DB"""
        job_exists = await self._db.exists(self._key)
        if job_exists == 0:
            return

        values = await self._db.hmget(self._key, *Control.ATTRIBUTES)

        for i, arg in enumerate(Control.ATTRIBUTES):
            val = values[i]

            if arg in Control.INT_ARGS:
                val = int(val)
            elif arg in Control.BOOL_ARGS:
                val = (val != 'False')

            setattr(self, arg, val)

    async def commit(self):
        """Commit object to DB"""
        key_vals = []
        for arg in Control.ATTRIBUTES:
            val = getattr(self, arg)
            if arg in Control.BOOL_ARGS:
                val = str(val)
            key_vals.extend([arg, val])

        return await self._db.hmset(self._key, *key_vals)
