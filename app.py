#!/usr/bin/env python3

import aws_cdk as cdk
from aws_cdk import Aspects
from cdk_nag import AwsSolutionsChecks, NagSuppressions


from infrastructure.kafka_confluent_validate import (
    KafkaConfluentValidateStack,
)

app = cdk.App()

stack = KafkaConfluentValidateStack(app, "kafka-confluent-validate")

cdk.Tags.of(stack).add("project", "kafka-confluent-validate")

NagSuppressions.add_stack_suppressions(
    stack,
    [
        {
            "id": "AwsSolutions-IAM4",
            "reason": "AWS Managed IAM policies have been allowed to maintain secured access with the ease of operational maintenance - however for more granular control the custom IAM policies can be used instead of AWS managed policies.",
        },
        {
            "id": "AwsSolutions-IAM5",
            "reason": "AWS managed policies in some cases use * in the resources field. AWS Managed IAM policies have been allowed to maintain secured access with the ease of operational maintenance - however for more granular control the custom IAM policies can be used instead of AWS managed policies.",
        },
        {
            "id": "AwsSolutions-SQS3",
            "reason": "The SQS queue in question is used as a de-facto DLQ.",
        },
    ],
)

Aspects.of(app).add(AwsSolutionsChecks())

app.synth()
