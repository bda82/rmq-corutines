import sys
import time
import logging

from src.consumers.base_consumer import BaseConsumer

logger = logging.getLogger(__name__)
LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name) -30s %(funcName) '
              '-35s %(lineno) -5d: %(message)s')
logging.basicConfig(level=logging.DEBUG, format=LOG_FORMAT)


def callback(body):
    logger.info(f'Get body on callback function: {body}')


class ParseConsumer:

    def __init__(self):
        self._reconnect_delay = 1
        self._consumer = BaseConsumer(consumer_name='PARSE_CONSUMER', callback_function=callback)

    def run(self):
        while True:
            try:
                self._consumer.run()
            except Exception as e:
                logger.error(f'Got an Exception: {e}')
                self._consumer.stop()
                break
            self._maybe_reconnect()

    def _maybe_reconnect(self):
        if self._consumer.should_reconnect:
            self._consumer.stop()
            reconnect_delay = self._get_reconnect_delay()
            logger.info(f'Reconnecting after {reconnect_delay} seconds')
            time.sleep(reconnect_delay)
            self._consumer = BaseConsumer(consumer_name='PARSE_CONSUMER', callback_function=callback)

    def _get_reconnect_delay(self):
        if self._consumer.was_consuming:
            self._reconnect_delay = 0
        else:
            self._reconnect_delay += 1
        if self._reconnect_delay > 30:
            self._reconnect_delay = 30
        return self._reconnect_delay


def start():
    try:
        consumer = ParseConsumer()
        consumer.run()
    except Exception as e:
        logger.error(f'Got an Exception: {e}')
        return 1
    return 0


if __name__ == '__main__':
    sys.exit(start())