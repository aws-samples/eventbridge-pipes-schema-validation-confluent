import os, base64, json
import pytest
import boto3
from confluent_kafka.serialization import SerializationContext, MessageField
from aws_lambda_powertools.utilities.typing import LambdaContext
from pydantic import BaseModel
from unittest import mock
from unittest.mock import call, ANY


with mock.patch.dict(
    "os.environ",
    {
        "DLQ_URL": "http://example.com/sqs",
        "CONFLUENT_SCHEMA_REGISTRY_SECRET_NAME": "confluent-schema-registry-secret",
    },
):
    from src import main


def test_decode_avro_value(mocker):
    def mock_deserialize(value, context):
        return "deserialized_value"

    mock_deserializer = mocker.Mock()
    mock_deserializer.side_effect = mock_deserialize

    mock_services = mocker.Mock()
    mock_services.avro_deserializer = mock_deserializer

    mocker.patch("src.main.services", mock_services)

    value = "aGVsbG8gd29ybGQ="
    topic = "topic_0"

    result = main.decode_avro_value(value, topic)

    assert result == "deserialized_value"

    mock_deserializer.assert_called_once_with(base64.b64decode(value), ANY)


def test_message_handler(mocker):
    mocker.patch("src.main.decode_string_key", return_value="decoded_key")
    mocker.patch("src.main.decode_avro_value", return_value="decoded_value")

    mock_message = mocker.Mock()
    mock_message.key = "MTI0MzU="
    mock_message.value = "AAABhqGmwgEKMTI0MzUKMTI0MzUAAAAAgEnIQA=="
    mock_message.topic = "topic_0"

    result = main.message_handler(mock_message)

    assert result.key == "decoded_key"
    assert result.value == "decoded_value"
    assert result.topic == "topic_0"

    main.decode_string_key.assert_called_once_with("MTI0MzU=")
    main.decode_avro_value.assert_called_once_with(
        "AAABhqGmwgEKMTI0MzUKMTI0MzUAAAAAgEnIQA==", "topic_0"
    )


class DeserializedMessage(BaseModel):
    key: str
    value: str
    topic: str


def test_lambda_handler(mocker):
    mock_services = mocker.Mock()
    mock_services.sqs = mocker.Mock()
    mocker.patch("src.main.services", mock_services)

    mocker.patch(
        "src.main.message_handler",
        return_value=DeserializedMessage(
            key="decoded_key",
            value="decoded_value",
            topic="topic_0",
        ),
    )

    mocker.patch("src.main.services.sqs.send_message")

    mock_context = mocker.Mock(spec=LambdaContext)

    event = [
        {
            "topic": "topic_0",
            "partition": 3,
            "offset": 2136,
            "timestamp": 1698221925140,
            "timestampType": "CREATE_TIME",
            "key": "MTI0MzU=",
            "value": "AAABhqGmwgEKMTI0MzUKMTI0MzUAAAAAgEnIQA==",
            "headers": [
                {"task.generation": [48]},
                {"task.id": [48]},
                {"current.iteration": [49, 50, 52, 51, 53]},
            ],
            "bootstrapServers": "smk://pkc-p11xm.us-east-1.aws.confluent.cloud:9092",
            "eventSource": "SelfManagedKafka",
            "eventSourceKey": "topic_0-3",
        }
    ]

    parsed_event = main.MessageList.parse_obj(event)

    result = main.lambda_handler(parsed_event, mock_context)

    expected_result = [
        {"key": "decoded_key", "value": "decoded_value", "topic": "topic_0"}
    ]

    assert result == expected_result

    main.message_handler.assert_called_once()
    main.services.sqs.send_message.assert_not_called()


def test_lambda_handler_deserialization_failure(mocker):
    mocker.patch.dict(os.environ, {"DLQ_URL": "http://example.com/sqs"})

    mock_services = mocker.Mock()
    mock_services.sqs = mocker.Mock()
    mocker.patch("src.main.services", mock_services)
    mocker.patch(
        "src.main.message_handler", side_effect=Exception("Deserialization Error")
    )

    mock_context = mocker.Mock(spec=LambdaContext)

    event = [
        {
            "topic": "topic_0",
            "partition": 3,
            "offset": 2136,
            "timestamp": 1698221925140,
            "timestampType": "CREATE_TIME",
            "key": "MTI0MzU=",
            "value": "AAABhqGmwgEKMTI0MzUKMTI0MzUAAAAAgEnIQA==",
            "headers": [
                {"task.generation": [48]},
                {"task.id": [48]},
                {"current.iteration": [49, 50, 52, 51, 53]},
            ],
            "bootstrapServers": "smk://pkc-p11xm.us-east-1.aws.confluent.cloud:9092",
            "eventSource": "SelfManagedKafka",
            "eventSourceKey": "topic_0-3",
        }
    ]

    parsed_event = main.MessageList.parse_obj(event)

    result = main.lambda_handler(parsed_event, mock_context)

    assert result == []

    main.message_handler.assert_called_once()

    main.services.sqs.send_message.assert_called_once_with(
        QueueUrl=os.environ["DLQ_URL"], MessageBody=ANY
    )

    message_body_arg = main.services.sqs.send_message.call_args[1]["MessageBody"]
    sent_message = json.loads(message_body_arg)
    assert sent_message == event[0]
