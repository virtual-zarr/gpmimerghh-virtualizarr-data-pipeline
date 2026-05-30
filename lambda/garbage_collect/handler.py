from datetime import datetime, timedelta, timezone

from aws_lambda_powertools import Logger
from virtualizarr_processor.processor import Processor

logger = Logger()


def handler() -> None:
    try:
        virtualizarr_processor = Processor()
        expiry_time = datetime.now(timezone.utc) - timedelta(hours=3)
        print(expiry_time)
        repo = virtualizarr_processor.initialize_repo()
        virtualizarr_processor.garbage_collect(expiry_time=expiry_time, repo=repo)
        logger.info("Icechunk garbage collected")
    except Exception as e:
        logger.error(f"Error in custom resource handler: {e}")
