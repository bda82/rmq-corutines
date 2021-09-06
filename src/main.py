import sys
import uvloop
import asyncio

from aiohttp.web import Application, run_app
import logging

from src.config.config import Config
from src.router import Router

from consumers.parse_consumer import start as start_parse_consumer

LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name) -30s %(funcName) '
              '-35s %(lineno) -5d: %(message)s')
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG, format=LOG_FORMAT)



uvloop.install()

config = Config()


def init_consumers():
    start_parse_consumer()


async def init_application(argv=None):
    logger.info('Run Server...')
    init_consumers()


async def init_aiohttp(argv=None):
    app = Application()
    Router.init(app)
    await init_application()
    return app


def main(argv=None):
    while True:
        try:
            run_app(init_aiohttp(argv))
            logger.info(f'[!!!] Exit Main loop...')
        except Exception as ex:
            logger.error(f'\n\n\n[!!!] Exit Main loop with: {ex}\n\n\n')
        finally:
            logger.info(f'\n\n\n[!!!] Try to restart server...\n\n\n')


if __name__ == '__main__':
    sys.exit(main(sys.argv))