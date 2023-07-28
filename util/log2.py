import logging
from functools import wraps
from kafka import KafkaProducer
from util.dataclient import Client


def createLogger(source_log: str = __name__, broker_hosts: str = 'localhost:9092', topic: str = 'logs'):
    """
    Função para criar um ponto de observação através do uso de logs

    :return:
    """
    log_format = '%(levelname)-8s||%(asctime)s||%(name)-12s||%(lineno)d||%(message)s'
    logging.basicConfig(level=logging.INFO, format=log_format)
    logger = logging.getLogger(source_log)
    producer = KafkaProducer(bootstrap_servers=[broker_hosts])

    def send_log_to_kafka(record):
        producer.send(topic, str(record).encode('utf-8'))

    # Replace the default logging handler with a custom one that sends logs to Kafka
    kafka_handler = logging.StreamHandler()
    kafka_handler.emit = send_log_to_kafka
    logger.addHandler(kafka_handler)

    return logger


def logs(func):
    """
    Decorator para monitorar via log qualquer função desejada
    :param func: não da função de entrada
    :return: retorna a função de entrada
    """

    @wraps(func)
    def inner(*args, **kwargs):
        logger = createLogger(func.__qualname__, topic='my_class_logs')
        # log_message = f'starting.... func:{func.__name__}:args:{args}:kwargs:{kwargs}'
        log_message = f'GENERAL/starting....'
        logger.info(log_message)
        result = func(*args, **kwargs)
        # log_message = f'finished func:{func.__name__}:args:{args}:kwargs:{kwargs}'
        log_message = f'GENERAL/finished.... '
        logger.info(log_message)
        return result

    return inner
