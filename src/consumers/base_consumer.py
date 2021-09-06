import functools
import logging
import time
import pika

from pika.adapters.asyncio_connection import AsyncioConnection
from pika.exchange_type import ExchangeType

from src.config.config import Config

LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name) -30s %(funcName) '
              '-35s %(lineno) -5d: %(message)s')
LOGGER = logging.getLogger(__name__)


class BaseConsumer:
    EXCHANGE = 'message'
    EXCHANGE_TYPE = ExchangeType.direct
    QUEUE = 'text'
    ROUTING_KEY = 'example.text'

    def __init__(self, consumer_name):
        self._logger = LOGGER

        self._config = Config().rmq
        self._credentials = pika.PlainCredentials(self._config.user,
                                                  self._config.password)
        self._parameters = pika.ConnectionParameters(self._config.host,
                                                     self._config.port,
                                                     self._config.virtual_host,
                                                     self._credentials,
                                                     heartbeat=300
                                                     )
        self.consumer_name = consumer_name
        self.should_reconnect = False
        self.was_consuming = False

        self._connection = None
        self._channel = None
        self._closing = False
        self._consumer_tag = None
        self._consuming = False
        # In production, experiment with higher prefetch values
        # for higher consumer throughput
        self._prefetch_count = 1

    def connect(self):
        self._logger.info(f'Connecting to RMQ in {self.consumer_name}')
        return AsyncioConnection(
            parameters=self._parameters,
            on_open_callback=self.on_connection_open,
            on_open_error_callback=self.on_connection_open_error,
            on_close_callback=self.on_connection_closed)

    def close_connection(self):
        self._consuming = False
        if self._connection.is_closing or self._connection.is_closed:
            self._logger.info(f'Connection is closing or already closed in {self.consumer_name}')
        else:
            self._logger.info(f'Closing connection in {self.consumer_name}')
            self._connection.close()

    def on_connection_open(self, _unused_connection):
        self._logger.info(f'Connection opened in {self.consumer_name}')
        self.open_channel()

    def on_connection_open_error(self, _unused_connection, err):
        self._logger.error(f'Connection open failed: {err} in {self.consumer_name}')
        self.reconnect()

    def on_connection_closed(self, _unused_connection, reason):
        self._channel = None
        if self._closing:
            self._connection.ioloop.stop()
        else:
            self._logger.warning(f'Connection closed, reconnect necessary by reason {reason}  in {self.consumer_name}')
            self.reconnect()

    def reconnect(self):
        self.should_reconnect = True
        self.stop()
        self._logger.info(f'Reconnect in {self.consumer_name}')

    def open_channel(self):
        self._logger.info(f'Creating a new channel in {self.consumer_name}')
        self._connection.channel(on_open_callback=self.on_channel_open)

    def on_channel_open(self, channel):
        self._logger.info(f'The Channel has been opened in {self.consumer_name}')
        self._channel = channel
        self.add_on_channel_close_callback()
        self.setup_exchange(self.EXCHANGE)

    def add_on_channel_close_callback(self):
        self._logger.info(f'Adding channel close callback in {self.consumer_name}')
        self._channel.add_on_close_callback(self.on_channel_closed)

    def on_channel_closed(self, channel, reason):
        self._logger.warning(f'Channel {channel} was closed by reason {reason}')
        self.close_connection()

    def setup_exchange(self, exchange_name):
        self._logger.info(f'Declaring exchange {exchange_name} in {self.consumer_name}')
        cb = functools.partial(
            self.on_exchange_declareok, userdata=exchange_name)
        self._channel.exchange_declare(
            exchange=exchange_name,
            exchange_type=self.EXCHANGE_TYPE,
            callback=cb)

    def on_exchange_declareok(self, _unused_frame, userdata):
        self._logger.info(f'Exchange declared {userdata} in {self.consumer_name}')
        self.setup_queue(self.QUEUE)

    def setup_queue(self, queue_name):
        self._logger.info(f'Declaring queue {queue_name} in {self.consumer_name}')
        cb = functools.partial(self.on_queue_declareok, userdata=queue_name)
        self._channel.queue_declare(queue=queue_name, callback=cb)

    def on_queue_declareok(self, _unused_frame, userdata):
        queue_name = userdata
        self._logger.info(f'Binding {self.EXCHANGE} to {queue_name} with {self.ROUTING_KEY} in {self.consumer_name}')
        cb = functools.partial(self.on_bindok, userdata=queue_name)
        self._channel.queue_bind(
            queue_name,
            self.EXCHANGE,
            routing_key=self.ROUTING_KEY,
            callback=cb)

    def on_bindok(self, _unused_frame, userdata):
        self._logger.info(f'Queue bound {userdata} in {self.consumer_name}', )
        self.set_qos()

    def set_qos(self):
        self._channel.basic_qos(prefetch_count=self._prefetch_count, callback=self.on_basic_qos_ok)

    def on_basic_qos_ok(self, _unused_frame):
        self._logger.info(f'QOS set to: {self._prefetch_count} in {self.consumer_name}')
        self.start_consuming()

