from dataclasses import dataclass
import inspect
from typing import Dict


@dataclass
class Client:
    """
    Classe usada para armazenar configuração inciial dos clientes para
    sendo composta por dicionarios,
    configura-se acesso a API, Cliente, database e process database
    TODO: Verificar a inclusao de entapas bronze, silver e gold
    """
    api: Dict
    Empresa: str
    database: Dict
    process_data: Dict
    provedor: Dict


def from_dict_to_dataclass(data: Dict, cls: Client = Client):
    """
    Realiza o prenchimento dos campos conforme lido no arquivo YALM ou Json Entrada
    :param cls: Classe que será criada
    :param data: Arquivo proveniente de um JSON ou YALM
    :return: Retorna um objeto data classe para ser manipulado ou acessado.
    """
    return cls(
        **{
            key: (data[key] if val.default == val.empty else data.get(key, val.default))
            for key, val in inspect.signature(cls).parameters.items()
        }
    )
