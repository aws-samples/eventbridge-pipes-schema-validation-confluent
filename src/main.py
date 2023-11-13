import os, base64, json
import boto3
from typing import List, Dict, Any
from aws_lambda_powertools import Logger, Tracer
from aws_lambda_powertools.utilities import parameters
from aws_lambda_powertools.utilities.typing import LambdaContext
from aws_lambda_powertools.utilities.parser import BaseModel, event_parser
from pydantic import RootModel
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer


logger = Logger()
tracer = Tracer()

services = None

try:
    DLQ_URL = os.environ["DLQ_URL"]
    CONFLUENT_SCHEMA_REGISTRY_SECRET_NAME = os.environ[
        "CONFLUENT_SCHEMA_REGISTRY_SECRET_NAME"
    ]
except Exception as e:
    logger.error({"Error retrieving environment variables": e})
    raise e


class Services:
    def __init__(self):
        self.sqs = boto3.client("sqs")
        try:
            self.schema_registry_secret = parameters.get_secret(
                CONFLUENT_SCHEMA_REGISTRY_SECRET_NAME, transform="json"
            )
        except Exception as e:
            logger.error({"Error retrieving Confluent Schema Registry secret": e})
            raise e

        try:
            self.schema_registry_client = SchemaRegistryClient(
                self.schema_registry_secret
            )
            self.avro_deserializer = AvroDeserializer(self.schema_registry_client)
        except Exception as e:
            logger.error({"Error creating Avro deserializer": e})
            raise e


class Message(BaseModel):
    eventSource: str
    bootstrapServers: str
    eventSourceKey: str
    topic: str
    partition: int
    offset: int
    timestamp: int
    timestampType: str
    key: str
    value: str
    headers: List[dict]


class MessageList(RootModel):
    root: List[Message]


@tracer.capture_method
def decode_string_key(key: str) -> str:
    decoded_key = base64.b64decode(key).decode()

    logger.debug({"decoded_key": decoded_key})

    return decoded_key


@tracer.capture_method
def decode_avro_value(value: str, topic: str) -> Any:
    decoded_value = base64.b64decode(value)

    deserialized_value = services.avro_deserializer(
        decoded_value, SerializationContext(topic, MessageField.VALUE)
    )

    logger.debug({"deserialized_value": deserialized_value})

    return deserialized_value


@tracer.capture_method
def message_handler(message: Any) -> Dict[str, Any]:
    logger.debug({"message": message})

    topic = message.topic

    decoded_message = message

    decoded_message.key = decode_string_key(message.key)
    decoded_message.value = decode_avro_value(message.value, topic)

    logger.debug(
        {
            "decoded_message": decoded_message,
        }
    )

    return decoded_message


@tracer.capture_lambda_handler
@logger.inject_lambda_context()
@event_parser(model=MessageList)
def lambda_handler(event: MessageList, context: LambdaContext) -> List[Dict[str, Any]]:
    global services

    if services is None:
        services = Services()

    logger.debug({"event": event})

    messages = event.root

    logger.info({"Number of messages to process": len(messages)})

    deserialized_messages = []
    error_messages = []
    for message in messages:
        try:
            deserialized_message = message_handler(message)
            deserialized_messages.append(deserialized_message)
        except Exception as e:
            logger.error({"Error deserializing message": e})
            error_messages.append(message)

    for message in error_messages:
        try:
            logger.debug({"Sending error message to DLQ": message})

            services.sqs.send_message(
                QueueUrl=DLQ_URL,
                MessageBody=json.dumps(message.dict()),
            )
        except Exception as e:
            logger.error({"Error sending message to DLQ": e})
            raise e

    logger.info(
        {
            "Number of messages deserialized": len(deserialized_messages),
            "Number of messages with errors": len(error_messages),
        }
    )

    return [message.dict() for message in deserialized_messages]
