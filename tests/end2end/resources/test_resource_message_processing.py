import time
from pathlib import Path
from unittest.mock import MagicMock

import jwt
import pytest

from chatops.services.chat_service import ChatService
from chatops.services.resource_service import ResourceService
from chatops.stream.event_stream import EventStream
from chatops.stream.job_stream import Job
from chatops.workers.response_generator import DOCUMENT_PROCESSED_RESPONSE, ResourceIngestion
from chatops.workers.worker import Worker
from conftest import sleep_until_message_timed_out

from .helpers import upload_resource
from ..helpers import create_chat, get_messages, stream_to_completion

DOCUMENTS_DIR = Path(__file__).parent / "fixtures" / "documents"


def _make_service(infra) -> ChatService:
    resource_service = ResourceService(resource_repository=infra["resource_repo"], resource_storage=infra["resource_storage"])
    return ChatService(chat_repository=infra["repo"], resource_service=resource_service)


def _decode_user_id(authed_client) -> str:
    token = authed_client.headers["Authorization"].removeprefix("Bearer ")
    return jwt.decode(token, options={"verify_signature": False})["sub"]


def test_ingestion_worker_completes_resource_ref_messages_on_create_follow_up_and_modify(
    authed_client_with_ingestion_worker,
):
    resource_id = upload_resource(authed_client_with_ingestion_worker)

    # a resource ref in the initial message is processed
    chat_id = create_chat(authed_client_with_ingestion_worker, f"[report.pdf](resource://{resource_id})")
    assistant_id = get_messages(authed_client_with_ingestion_worker, chat_id)[1]["id"]
    stream_to_completion(authed_client_with_ingestion_worker, chat_id, assistant_id)

    messages = get_messages(authed_client_with_ingestion_worker, chat_id)
    assert messages[1]["id"] == assistant_id
    assert messages[1]["status"] == "complete"
    assert messages[1]["content"] == DOCUMENT_PROCESSED_RESPONSE

    # a resource ref in a follow-up message is processed the same way
    authed_client_with_ingestion_worker.post(
        f"/api/chats/{chat_id}/messages", json={"content": f"[report.pdf](resource://{resource_id})"},
    )
    follow_up_assistant_id = get_messages(authed_client_with_ingestion_worker, chat_id)[3]["id"]
    stream_to_completion(authed_client_with_ingestion_worker, chat_id, follow_up_assistant_id)

    messages = get_messages(authed_client_with_ingestion_worker, chat_id)
    assert messages[3]["id"] == follow_up_assistant_id
    assert messages[3]["status"] == "complete"
    assert messages[3]["content"] == DOCUMENT_PROCESSED_RESPONSE

    # a resource ref added via modify, in a fresh chat, is processed the same way
    other_chat_id = create_chat(authed_client_with_ingestion_worker, f"[report.pdf](resource://{resource_id})")
    other_user_message_id = get_messages(authed_client_with_ingestion_worker, other_chat_id)[0]["id"]
    other_first_assistant_id = get_messages(authed_client_with_ingestion_worker, other_chat_id)[1]["id"]
    stream_to_completion(authed_client_with_ingestion_worker, other_chat_id, other_first_assistant_id)

    modify_response = authed_client_with_ingestion_worker.post(
        f"/api/chats/{other_chat_id}/messages/{other_user_message_id}/modify",
        json={"content": f"Please check [report.pdf](resource://{resource_id})"},
    )
    assert modify_response.status_code == 200
    new_assistant_id = modify_response.json()["id"]

    stream_to_completion(authed_client_with_ingestion_worker, other_chat_id, new_assistant_id)

    messages = get_messages(authed_client_with_ingestion_worker, other_chat_id)
    assert messages[1]["id"] == new_assistant_id
    assert messages[1]["status"] == "complete"
    assert messages[1]["content"] == DOCUMENT_PROCESSED_RESPONSE


@pytest.mark.parametrize(
    "settings",
    [{"message_timeout": {"resource_processing_timeout": 0.05}}],
    indirect=True,
)
def test_retry_of_failed_resource_ref_message_is_processed_by_ingestion_worker(authed_client, settings, infra, request):
    resource_id = upload_resource(authed_client)

    chat_id = create_chat(authed_client, f"[report.pdf](resource://{resource_id})")
    assistant_message = get_messages(authed_client, chat_id)[1]
    assistant_id = assistant_message["id"]

    sleep_until_message_timed_out(assistant_message, settings.message_timeout)  # no worker running yet, so the message times out and fails
    assert get_messages(authed_client, chat_id)[1]["status"] == "failed"
    infra["ingestion_job_stream"].consume()  # drain the original job, never picked up before the timeout

    request.getfixturevalue("ingestion_worker")
    retry_response = authed_client.post(f"/api/chats/{chat_id}/messages/{assistant_id}/retry")
    assert retry_response.status_code == 200

    stream_to_completion(authed_client, chat_id, assistant_id)

    messages = get_messages(authed_client, chat_id)
    assert messages[1]["id"] == assistant_id
    assert messages[1]["status"] == "complete"
    assert messages[1]["content"] == DOCUMENT_PROCESSED_RESPONSE


@pytest.mark.parametrize("settings", [{"message_timeout": {"resource_processing_timeout": 0.05}}], indirect=True)
def test_ingestion_worker_discards_stale_job_for_message_already_failed_by_timeout(authed_client, settings, request) -> None:
    resource_id = upload_resource(authed_client, "doc.pdf")
    chat_id = create_chat(authed_client, f"[doc.pdf](resource://{resource_id})")
    assistant_message = get_messages(authed_client, chat_id)[1]

    sleep_until_message_timed_out(assistant_message, settings.message_timeout)  # no ingestion worker running yet, so this message's job sits stale in the queue
    assert get_messages(authed_client, chat_id)[1]["status"] == "failed"

    other_resource_id = upload_resource(authed_client, "other.pdf")
    other_chat_id = create_chat(authed_client, f"[other.pdf](resource://{other_resource_id})")
    other_assistant_id = get_messages(authed_client, other_chat_id)[1]["id"]

    request.getfixturevalue("ingestion_worker")  # starts consuming: stale job first, then the fresh one

    stream_to_completion(authed_client, other_chat_id, other_assistant_id)
    assert get_messages(authed_client, other_chat_id)[1]["status"] == "complete"

    stale_message = get_messages(authed_client, chat_id)[1]
    assert stale_message["status"] == "failed"
    assert stale_message["content"] == ""


def test_ingestion_worker_discards_job_for_nonexistent_message(authed_client, infra, ingestion_worker) -> None:
    user_id = _decode_user_id(authed_client)
    chat_id = create_chat(authed_client, "Hello")

    infra["ingestion_job_stream"].publish(
        Job(chat_id=chat_id, user_id=user_id, message_id="does-not-exist", resource_ids=("res-1",))
    )

    # worker must discard the bogus job and keep processing subsequent ones
    resource_id = upload_resource(authed_client, "doc.pdf")
    other_chat_id = create_chat(authed_client, f"[doc.pdf](resource://{resource_id})")
    other_assistant_id = get_messages(authed_client, other_chat_id)[1]["id"]

    stream_to_completion(authed_client, other_chat_id, other_assistant_id)
    assert get_messages(authed_client, other_chat_id)[1]["status"] == "complete"


def test_ingestion_worker_fails_message_on_processing_exception(authed_client, infra) -> None:
    resource_id = upload_resource(authed_client, "doc.pdf")
    chat_id = create_chat(authed_client, f"[doc.pdf](resource://{resource_id})")

    broken_event_stream = MagicMock(spec=EventStream)
    broken_event_stream.write.side_effect = RuntimeError("boom")
    worker = Worker(
        jobs_stream=infra["ingestion_job_stream"], chat_service=_make_service(infra), event_stream=broken_event_stream,
        response_generator=ResourceIngestion(),
    ).start()
    try:
        status = None
        for _ in range(20):
            status = get_messages(authed_client, chat_id)[1]["status"]
            if status == "failed":
                break
            time.sleep(0.05)
    finally:
        worker.stop()

    assert status == "failed"


@pytest.mark.parametrize(
    "settings",
    [{
        "event_stream_timeout": 0.05,
        "message_timeout": {"message_generation_timeout": 0.05, "resource_processing_timeout": 0.2},
    }],
    indirect=True,
)
def test_stream_uses_resource_processing_timeout_for_resource_messages(authed_client):
    resource_id = upload_resource(authed_client)
    chat_id = create_chat(authed_client, f"[report.pdf](resource://{resource_id})")
    assistant_id = get_messages(authed_client, chat_id)[1]["id"]

    start = time.monotonic()
    lines = stream_to_completion(authed_client, chat_id, assistant_id)
    elapsed = time.monotonic() - start

    assert "event: error" in lines
    assert elapsed > 0.1  # governed by resource_processing_timeout (0.2s), not message_generation_timeout (0.05s)


def test_generate_answers_question_about_uploaded_document(authed_client, llm_ingestion_worker, llm_worker) -> None:
    pdf_bytes = (DOCUMENTS_DIR / "report.pdf").read_bytes()
    resource_id = upload_resource(authed_client, "report.pdf", content=pdf_bytes)

    chat_id = create_chat(authed_client, f"[report.pdf](resource://{resource_id})")
    ingestion_assistant_id = get_messages(authed_client, chat_id)[1]["id"]
    stream_to_completion(authed_client, chat_id, ingestion_assistant_id)
    assert get_messages(authed_client, chat_id)[1]["status"] == "complete"

    authed_client.post(
        f"/api/chats/{chat_id}/messages",
        json={"content": "What is the launch code mentioned in the document? Reply with just the code, nothing else."},
    )
    answer_id = get_messages(authed_client, chat_id)[3]["id"]
    stream_to_completion(authed_client, chat_id, answer_id)

    messages = get_messages(authed_client, chat_id)
    assert messages[3]["status"] == "complete"
    assert "bluebird-7" in messages[3]["content"].lower()
