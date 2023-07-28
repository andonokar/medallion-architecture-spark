import logging
from functools import wraps, partial
from util.dataclient import Client


def createLogger(source_log: str = __name__):
    """
    Função para criar um ponto de observaçã atravé do uso de logs

    :return:
    """
    # handler = logging_loki.LokiHandler(
    #     url="http://localhost:3100/loki/api/v1/push",
    #     tags={"application": client},
    #     # auth=("username", "password"),
    #     version="1",
    # )

    log_format = '%(levelname)-8s||%(asctime)s||%(name)-12s||%(lineno)d||%(message)s'
    logging.basicConfig(level=logging.INFO, format=log_format)
    logger = logging.getLogger(source_log)
    # logger.addHandler(handler)
    return logger


def message_error(client: Client, message: str) -> None:
    msg_log = createLogger()
    msg_log.error(message)


def logs(func):
    """
    Decorator para monitorar vi log qualquer função desejada
    :param func: não da função de entrada
    :return: retorna a função de entrada
    """

    @wraps(func)
    def inner(*args, **kwargs):
        logger = createLogger(func.__name__)
        # log_message = f'stating.... func:{func.__name__}:args:{args}:kwargs:{kwargs}'
        log_message = f'stating....'
        logger.info(log_message)
        result = func(*args, **kwargs)
        # log_message = f'finished func:{func.__name__}:args:{args}:kwargs:{kwargs}'
        log_message = f'finished.... '
        logger.info(log_message)
        return result

    return inner


