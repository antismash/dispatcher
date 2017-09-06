"""Signals to use within the application"""
import asyncio


class Signal(list):
    """Async signal implementation

    Inspired by the signal handling in aiohttp
    """
    __slots__ = ('_app', )

    def __init__(self, app):
        """Connect the signal to the app

        :param app: app the signal is conencted to
        """
        super().__init__()
        self._app = app

    async def send(self, *args, **kwargs):
        """Send args and kwargs to all registered callbacks"""
        for callback in self:
            res = callback(*args, **kwargs)
            if asyncio.iscoroutine(res) or isinstance(res, asyncio.Future):
                await res

