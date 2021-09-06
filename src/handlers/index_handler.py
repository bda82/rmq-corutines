from json import JSONDecodeError

from aiohttp import web
from aiohttp_cors import CorsViewMixin


class IndexView(web.View, CorsViewMixin):

    async def get(self):    # noqa
        return web.Response(text='System is online.', status=web.HTTPOk().status_code)
