"""Database connectivity"""

import redis.asyncio as redis


class DatabaseConfig:
    """Class collecting all the database-related configuraion"""
    __slots__ = ('uri')

    def __init__(self, uri: str):
        self.uri = uri

    @classmethod
    def from_argparse(cls, args):
        """Initialise a database config from argparse

        :param args: argparse.Namespace containing the database-related settings
        """
        assert args.db.startswith('redis')

        return cls(args.db)


async def init_db(app):
    """Initialize the database connection

    :param app: Application to init the database for
    """
    conf = app['db_conf']
    app.logger.debug("Connecting to %s", conf.uri)

    engine = await redis.Redis.from_url(conf.uri, encoding='utf-8', decode_responses=True)
    app['engine'] = engine


async def close_db(app):
    """Close the redis connection

    :param app: App to close the connection for
    """
    engine = app['engine']
    app.logger.debug("Closing redis connection")
    await engine.close()
