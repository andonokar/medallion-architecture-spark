from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import (
    SerializationContext,
    MessageField,
)
import json
from datetime import datetime
from variables import kafka_config


def createloggerforkafka(message, level, topic, **kwargs):
    """
    Função para criar um ponto de observação através do uso de logs e enviar ao kafka
    :return:
    """
    schema_registry_conf = kafka_config.get('schema_registry')
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)
    producer = Producer(kafka_config.get('producer_config'))
    avro_schema_str = json.dumps(kafka_config.get('avro_log_schema'))
    avro_serializer = AvroSerializer(schema_registry_client, avro_schema_str)

    message_payload = {
        "data_hora": datetime.now().strftime("%Y-%m-%d %H:%M"),
        "mensagem": message,
        "log_mensagem": level
    }
    message_payload = {**message_payload, **kwargs}
    producer.produce(topic=topic, value=avro_serializer(message_payload, SerializationContext(topic, MessageField.VALUE)))
    producer.flush()
