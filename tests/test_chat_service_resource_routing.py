import time

import pytest

from chatops.domain.chat import MessageStatus
from chatops.domain.resource import Resource
from chatops.services.chat_service import ChatService
from chatops.services.resource_service import ResourceAccessDeniedError, ResourceNotFoundError, ResourceService
from chatops.stream.ingestion_job_stream import RedisIngestionJobStream
from chatops.stream.job_stream import RedisJobStream

USER_ID = "test-user"
OTHER_USER_ID = "other-user"


def _make_service(infra) -> ChatService:
    resource_service = ResourceService(resource_repository=infra["resource_repo"], resource_storage=infra["resource_storage"])
    return ChatService(chat_repository=infra["repo"], resource_service=resource_service)


def _make_streams(infra):
    return (
        RedisJobStream(infra["redis_client"], timeout=0.1),
        RedisIngestionJobStream(infra["redis_client"], timeout=0.1),
    )


def _seed_resource(infra, resource_id: str, user_id: str) -> None:
    infra["resource_repo"].save_resource(
        Resource(
            id=resource_id,
            user_id=user_id,
            filename="doc.pdf",
            file_path=f"/data/resources/{resource_id}",
            created_at=int(time.time() * 1000),
        )
    )


def _start_chat(service: ChatService, infra, jobs_stream, ingestion_jobs):
    chat = service.create_chat("Hello", USER_ID, jobs_stream, ingestion_jobs)
    jobs_stream.consume()  # drain create_chat's own assistant job
    pending = service.fetch_messages(chat.id, USER_ID)[1]
    infra["repo"].save_message(chat.id, pending.model_copy(update={"status": MessageStatus.COMPLETE, "content": "Done"}))
    return chat


def test_message_without_resource_refs_publishes_assistant_job_only(infra) -> None:
    service = _make_service(infra)
    jobs_stream, ingestion_jobs = _make_streams(infra)
    chat = _start_chat(service, infra, jobs_stream, ingestion_jobs)

    assistant = service.send_message(chat.id, USER_ID, "plain text, no refs", jobs_stream, ingestion_jobs)

    job = jobs_stream.consume()
    assert job.message_id == assistant.id
    with pytest.raises(TimeoutError):
        ingestion_jobs.consume()


def test_message_with_owned_resource_ref_publishes_ingestion_job_only(infra) -> None:
    service = _make_service(infra)
    jobs_stream, ingestion_jobs = _make_streams(infra)
    chat = _start_chat(service, infra, jobs_stream, ingestion_jobs)
    _seed_resource(infra, "res-1", USER_ID)

    assistant = service.send_message(chat.id, USER_ID, "[doc.pdf](resource://res-1)", jobs_stream, ingestion_jobs)

    job = ingestion_jobs.consume()
    assert job.message_id == assistant.id
    assert job.resource_ids == ("res-1",)
    with pytest.raises(TimeoutError):
        jobs_stream.consume()


def test_message_mixing_ref_with_plain_text_routes_to_ingestion(infra) -> None:
    service = _make_service(infra)
    jobs_stream, ingestion_jobs = _make_streams(infra)
    chat = _start_chat(service, infra, jobs_stream, ingestion_jobs)
    _seed_resource(infra, "res-1", USER_ID)

    assistant = service.send_message(
        chat.id, USER_ID, "Please review [doc.pdf](resource://res-1) soon", jobs_stream, ingestion_jobs
    )

    job = ingestion_jobs.consume()
    assert job.message_id == assistant.id
    assert job.resource_ids == ("res-1",)


def test_message_with_multiple_refs_publishes_single_job_with_all_ids(infra) -> None:
    service = _make_service(infra)
    jobs_stream, ingestion_jobs = _make_streams(infra)
    chat = _start_chat(service, infra, jobs_stream, ingestion_jobs)
    _seed_resource(infra, "res-1", USER_ID)
    _seed_resource(infra, "res-2", USER_ID)

    assistant = service.send_message(
        chat.id, USER_ID, "[a.pdf](resource://res-1) [b.pdf](resource://res-2)", jobs_stream, ingestion_jobs
    )

    job = ingestion_jobs.consume()
    assert job.message_id == assistant.id
    assert job.resource_ids == ("res-1", "res-2")
    with pytest.raises(TimeoutError):
        ingestion_jobs.consume()  # only a single job was published


def test_message_with_nonexistent_resource_ref_raises_and_creates_nothing(infra) -> None:
    service = _make_service(infra)
    jobs_stream, ingestion_jobs = _make_streams(infra)
    chat = _start_chat(service, infra, jobs_stream, ingestion_jobs)

    with pytest.raises(ResourceNotFoundError):
        service.send_message(chat.id, USER_ID, "[doc.pdf](resource://missing)", jobs_stream, ingestion_jobs)

    assert len(service.fetch_messages(chat.id, USER_ID)) == 2
    with pytest.raises(TimeoutError):
        jobs_stream.consume()
    with pytest.raises(TimeoutError):
        ingestion_jobs.consume()


def test_message_with_another_users_resource_ref_raises_and_creates_nothing(infra) -> None:
    service = _make_service(infra)
    jobs_stream, ingestion_jobs = _make_streams(infra)
    chat = _start_chat(service, infra, jobs_stream, ingestion_jobs)
    _seed_resource(infra, "res-1", OTHER_USER_ID)

    with pytest.raises(ResourceAccessDeniedError):
        service.send_message(chat.id, USER_ID, "[doc.pdf](resource://res-1)", jobs_stream, ingestion_jobs)

    assert len(service.fetch_messages(chat.id, USER_ID)) == 2
    with pytest.raises(TimeoutError):
        jobs_stream.consume()
    with pytest.raises(TimeoutError):
        ingestion_jobs.consume()


def test_retry_message_with_resource_ref_routes_to_ingestion(infra) -> None:
    service = _make_service(infra)
    jobs_stream, ingestion_jobs = _make_streams(infra)
    chat = _start_chat(service, infra, jobs_stream, ingestion_jobs)
    _seed_resource(infra, "res-1", USER_ID)

    assistant = service.send_message(chat.id, USER_ID, "[doc.pdf](resource://res-1)", jobs_stream, ingestion_jobs)
    ingestion_jobs.consume()  # drain the original ingestion job
    service.fail_message(chat.id, USER_ID, assistant.id)

    retried = service.retry_message(chat.id, USER_ID, assistant.id, jobs_stream, ingestion_jobs)

    job = ingestion_jobs.consume()
    assert job.message_id == retried.id
    assert job.resource_ids == ("res-1",)
    with pytest.raises(TimeoutError):
        jobs_stream.consume()


def test_modify_message_with_new_resource_ref_routes_to_ingestion(infra) -> None:
    service = _make_service(infra)
    jobs_stream, ingestion_jobs = _make_streams(infra)
    chat = _start_chat(service, infra, jobs_stream, ingestion_jobs)
    _seed_resource(infra, "res-1", USER_ID)

    user_message = service.fetch_messages(chat.id, USER_ID)[0]
    assistant = service.modify_message(
        chat.id, USER_ID, user_message.id, "[doc.pdf](resource://res-1)", jobs_stream, ingestion_jobs,
    )

    job = ingestion_jobs.consume()
    assert job.message_id == assistant.id
    assert job.resource_ids == ("res-1",)
    with pytest.raises(TimeoutError):
        jobs_stream.consume()
