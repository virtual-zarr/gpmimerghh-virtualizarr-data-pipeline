import os
from typing import Any, Literal

from pydantic import model_validator
from pydantic_settings import BaseSettings

print("STAGE from env:", os.getenv("STAGE"))


def include_trailing_slash(value: Any) -> Any:
    """Make sure the value includes a trailing slash if str"""
    if isinstance(value, str):
        return value.rstrip("/") + "/"
    return value


class StackSettings(BaseSettings):
    PROJECT_NAME: str = "virtualizarr-data-pipelines"
    STACK_NAME: str = "virtualizarr-data-pipelines"
    STAGE: Literal["dev", "prod"]
    ACCOUNT_ID: str
    ACCOUNT_REGION: str = "us-west-2"
    ICECHUNK_BUCKET_NAME: str = "icechunk-outuput"
    ICECHUNK_BUCKET: str | None = None
    ICECHUNK_PREFIX: str | None = None
    DATA_BUCKET_NAME: str | None = None
    PROJECT: str = "virtualizarr-data-pipelines"
    SNS_TOPIC: str | None = None
    MAX_CONCURRENCY: int = 50
    SQS_BATCH_SIZE: int = 10
    EARTHDATA_SECRET_ARN: str | None = None

    # process_messages Lambda tuning. Raise LAMBDA_TIMEOUT toward 900 (the
    # Lambda max) to fit larger SQS_BATCH_SIZE values; commits serialize on
    # main, so bigger batches (fewer commits) scale better than more concurrency.
    LAMBDA_TIMEOUT: int = 300
    LAMBDA_MEMORY: int = 2048
    # SQS visibility timeout. Must be >= LAMBDA_TIMEOUT so a record isn't
    # redelivered while its batch is still being processed.
    VISIBILITY_TIMEOUT: int = 1800

    # Freguency in days to run garbage collection.
    GARBAGE_COLLECTION_FREQUENCY: int | None = None

    VPC_ID: str | None = None
    # AWS Batch cluster reference to SSM parameter describing the AMI _or_ the AMI ID
    # If using SSM to resolve the AMI ID, prefix with `resolve:ssm`.
    # MCP_AMI_ID: str = "resolve:ssm:/mcp/amis/aml2023-ecs"
    AMI_ID: str = (
        "resolve:ssm:/aws/service/ecs/optimized-ami/amazon-linux-2/recommended/image_id"
    )

    # Cluster scaling max
    BATCH_MAX_VCPU: int = 10

    @model_validator(mode="after")
    def _check_visibility_timeout(self) -> "StackSettings":
        if self.VISIBILITY_TIMEOUT < self.LAMBDA_TIMEOUT:
            raise ValueError(
                f"VISIBILITY_TIMEOUT ({self.VISIBILITY_TIMEOUT}s) must be >= "
                f"LAMBDA_TIMEOUT ({self.LAMBDA_TIMEOUT}s)"
            )
        return self
