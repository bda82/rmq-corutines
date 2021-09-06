from aiohttp.web import Application

from src.handlers.index_handler import IndexView


class Router:

    def __init__(self):
        pass

    @staticmethod
    def init(app: Application) -> None:
        app.router.add_view('/test-broker', IndexView)
