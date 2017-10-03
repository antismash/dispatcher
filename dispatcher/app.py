"""Standalone application class"""
import asyncio
from .signal import Signal
from .log import core_logger


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
        self._started_tasks = []
        self.tasks = []
        self.main_task = None

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

    def start_task(self, func):
        """Start up a task"""
        task = self.loop.create_task(func(self))
        self._started_tasks.append(task)

        def done_callback(done_task):
            self._started_tasks.remove(done_task)

        task.add_done_callback(done_callback)
        return task

    def run(self, loop=None):
        """Actually run the application

        :param loop: Custom event loop or None for default
        """
        if loop is None:
            loop = asyncio.get_event_loop()

        self.loop = loop

        loop.run_until_complete(self.startup())

        for func in self.tasks:
            self.start_task(func)

        try:
            task = self.start_task(self.main_task)
            loop.run_until_complete(task)
        except (KeyboardInterrupt, SystemError):
            print("Attempting graceful shutdown, press Ctrl-C again to exit", flush=True)

            def shutdown_exception_handler(_loop, context):
                if "exception" not in context or not isinstance(context["exception"], asyncio.CancelledError):
                    _loop.default_exception_handler(context)
            loop.set_exception_handler(shutdown_exception_handler)

            tasks = asyncio.gather(*self._started_tasks, loop=loop, return_exceptions=True)
            tasks.add_done_callback(lambda _: loop.stop())
            tasks.cancel()

            while not tasks.done() and not loop.is_closed():
                loop.run_forever()
        finally:
            loop.run_until_complete(self.shutdown())
            loop.run_until_complete(self.cleanup())
            loop.close()
