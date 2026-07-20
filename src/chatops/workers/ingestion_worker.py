import logging

from chatops.services.chat_service import ChatService
from chatops.workers.response_generator import ResourceIngestion
from chatops.workers.worker import Worker

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    from chatops.api.dependencies import (
        get_chat_repository,
        get_event_stream,
        get_ingestion_job_stream,
        get_resource_repository,
        get_resource_service,
        get_resource_storage,
    )

    chat_service = ChatService(
        chat_repository=get_chat_repository(),
        resource_service=get_resource_service(repo=get_resource_repository(), storage=get_resource_storage()),
    )

    Worker(
        jobs_stream=get_ingestion_job_stream(),
        chat_service=chat_service,
        event_stream=get_event_stream(),
        response_generator=ResourceIngestion(processing_delay=15.0),
    ).start().join()
