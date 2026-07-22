import logging

from chatops.services.chat_service import ChatService
from chatops.workers.llm_resource_ingestion import LLMResourceIngestion
from chatops.workers.worker import Worker

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    from chatops.api.dependencies import (
        get_chat_repository,
        get_event_stream,
        get_ingestion_job_stream,
        get_openai_client,
        get_resource_repository,
        get_resource_service,
        get_resource_storage,
        get_settings,
    )

    resource_service = get_resource_service(repo=get_resource_repository(), storage=get_resource_storage())
    chat_service = ChatService(chat_repository=get_chat_repository(), resource_service=resource_service)

    Worker(
        jobs_stream=get_ingestion_job_stream(),
        chat_service=chat_service,
        event_stream=get_event_stream(),
        response_generator=LLMResourceIngestion(
            resource_service=resource_service, client=get_openai_client(), model=get_settings().openai_model,
        ),
    ).start().join()
