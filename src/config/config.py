import os


class RMQConfig:
    host = ''
    port = ''
    user = ''
    password = ''
    virtual_host = ''
    delivery_mode = ''
    prefetch_count = ''

    # Queues
    connector_queue = 'connector_queue'
    parser_queue = 'parser_queue'


class Config:

    def __init__(self):
        self.rmq = RMQConfig()
        self.rmq.host = os.environ.get('RABBITMQ_HOST', 'rabbitmq')
        self.rmq.port = os.environ.get('RABBITMQ_PORT', 5672)
        self.rmq.user = os.environ.get('RABBITMQ_DEFAULT_USER', 'root')
        self.rmq.password = os.environ.get('RABBITMQ_DEFAULT_PASS', 'password')
        self.rmq.virtual_host = os.environ.get('RABBITMQ_DEFAULT_VIRTUAL_HOST', '/')
        self.rmq.delivery_mode = os.environ.get('RABBITMQ_DEFAULT_DELIVERY_MODE', 2)
        self.rmq.prefetch_count = os.environ.get('RABBITMQ_DEFAULT_PREFETCH_COUNT', 0)
