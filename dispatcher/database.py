"""Database connectivity"""

import aioredis


class DatabaseConfig:
    """Class collecting all the database-related configuraion"""
    __slots__ = ('host', 'port', 'db', 'max_conns')

    def __init__(self, host, port, db, max_conns):
        self.host = host
        self.port = port
        self.db = db
        self.max_conns = max_conns

    @classmethod
    def from_argparse(cls, args):
        """Initialise a database config from argparse

        :param args: argparse.Namespace containing the database-related settings
        """
        assert args.db.startswith('redis://')

        # skip the protocol
        queue = args.db[8:]

        parts = queue.split('/')
        db = 0 if len(parts) < 2 else int(parts[-1])

        parts = parts[0].split(':')

        port = 6379 if len(parts) < 2 else int(parts[-1])

        host = parts[0]

        max_conns = (args.max_jobs * 2) + 5

        return cls(host, port, db, max_conns)


async def init_db(app):
    """Initialize the database connection

    :param app: Application to init the database for
    """
    conf = app['db_conf']
    app.logger.debug("Connecting to redis://%s:%s/%s", conf.host, conf.port, conf.db)

    engine = await aioredis.create_pool((conf.host, conf.port), db=conf.db, encoding='utf-8',
                                        maxsize=conf.max_conns, loop=app.loop)
    app['engine'] = engine


async def close_db(app):
    """Close the redis connection

    :param app: App to close the connection for
    """
    engine = app['engine']
    app.logger.debug("Closing redis connection")
    engine.close()
    await engine.wait_closed()

