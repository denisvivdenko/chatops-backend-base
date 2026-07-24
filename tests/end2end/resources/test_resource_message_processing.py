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
    chat_id = authed_client_with_ingestion_worker.post(
        "/api/chats", json={"message": f"[report.pdf](resource://{resource_id})"},
    ).json()["id"]
    assistant_id = authed_client_with_ingestion_worker.get(f"/api/chats/{chat_id}/messages").json()[1]["id"]
    with authed_client_with_ingestion_worker.stream(
        "GET", f"/api/chats/{chat_id}/messages/{assistant_id}/stream"
    ) as resp:
        list(resp.iter_lines())

    messages = authed_client_with_ingestion_worker.get(f"/api/chats/{chat_id}/messages").json()
    assert messages[1]["id"] == assistant_id
    assert messages[1]["status"] == "complete"
    assert messages[1]["content"] == DOCUMENT_PROCESSED_RESPONSE

    # a resource ref in a follow-up message is processed the same way
    authed_client_with_ingestion_worker.post(
        f"/api/chats/{chat_id}/messages", json={"content": f"[report.pdf](resource://{resource_id})"},
    )
    follow_up_assistant_id = authed_client_with_ingestion_worker.get(f"/api/chats/{chat_id}/messages").json()[3]["id"]
    with authed_client_with_ingestion_worker.stream(
        "GET", f"/api/chats/{chat_id}/messages/{follow_up_assistant_id}/stream"
    ) as resp:
        list(resp.iter_lines())

    messages = authed_client_with_ingestion_worker.get(f"/api/chats/{chat_id}/messages").json()
    assert messages[3]["id"] == follow_up_assistant_id
    assert messages[3]["status"] == "complete"
    assert messages[3]["content"] == DOCUMENT_PROCESSED_RESPONSE

    # a resource ref added via modify, in a fresh chat, is processed the same way
    other_chat_id = authed_client_with_ingestion_worker.post(
        "/api/chats", json={"message": f"[report.pdf](resource://{resource_id})"},
    ).json()["id"]
    other_user_message_id = authed_client_with_ingestion_worker.get(f"/api/chats/{other_chat_id}/messages").json()[0]["id"]
    other_first_assistant_id = authed_client_with_ingestion_worker.get(f"/api/chats/{other_chat_id}/messages").json()[1]["id"]
    with authed_client_with_ingestion_worker.stream(
        "GET", f"/api/chats/{other_chat_id}/messages/{other_first_assistant_id}/stream"
    ) as resp:
        list(resp.iter_lines())

    modify_response = authed_client_with_ingestion_worker.post(
        f"/api/chats/{other_chat_id}/messages/{other_user_message_id}/modify",
        json={"content": f"Please check [report.pdf](resource://{resource_id})"},
    )
    assert modify_response.status_code == 200
    new_assistant_id = modify_response.json()["id"]

    with authed_client_with_ingestion_worker.stream(
        "GET", f"/api/chats/{other_chat_id}/messages/{new_assistant_id}/stream"
    ) as resp:
        list(resp.iter_lines())

    messages = authed_client_with_ingestion_worker.get(f"/api/chats/{other_chat_id}/messages").json()
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

    chat_id = authed_client.post(
        "/api/chats", json={"message": f"[report.pdf](resource://{resource_id})"},
    ).json()["id"]
    assistant_message = authed_client.get(f"/api/chats/{chat_id}/messages").json()[1]
    assistant_id = assistant_message["id"]

    sleep_until_message_timed_out(assistant_message, settings.message_timeout)  # no worker running yet, so the message times out and fails
    assert authed_client.get(f"/api/chats/{chat_id}/messages").json()[1]["status"] == "failed"
    infra["ingestion_job_stream"].consume()  # drain the original job, never picked up before the timeout

    request.getfixturevalue("ingestion_worker")
    retry_response = authed_client.post(f"/api/chats/{chat_id}/messages/{assistant_id}/retry")
    assert retry_response.status_code == 200

    with authed_client.stream("GET", f"/api/chats/{chat_id}/messages/{assistant_id}/stream") as resp:
        list(resp.iter_lines())

    messages = authed_client.get(f"/api/chats/{chat_id}/messages").json()
    assert messages[1]["id"] == assistant_id
    assert messages[1]["status"] == "complete"
    assert messages[1]["content"] == DOCUMENT_PROCESSED_RESPONSE


@pytest.mark.parametrize("settings", [{"message_timeout": {"resource_processing_timeout": 0.05}}], indirect=True)
def test_ingestion_worker_discards_stale_job_for_message_already_failed_by_timeout(authed_client, settings, request) -> None:
    resource_id = upload_resource(authed_client, "doc.pdf")
    chat_id = authed_client.post("/api/chats", json={"message": f"[doc.pdf](resource://{resource_id})"}).json()["id"]
    assistant_message = authed_client.get(f"/api/chats/{chat_id}/messages").json()[1]

    sleep_until_message_timed_out(assistant_message, settings.message_timeout)  # no ingestion worker running yet, so this message's job sits stale in the queue
    assert authed_client.get(f"/api/chats/{chat_id}/messages").json()[1]["status"] == "failed"

    other_resource_id = upload_resource(authed_client, "other.pdf")
    other_chat_id = authed_client.post(
        "/api/chats", json={"message": f"[other.pdf](resource://{other_resource_id})"}
    ).json()["id"]
    other_assistant_id = authed_client.get(f"/api/chats/{other_chat_id}/messages").json()[1]["id"]

    request.getfixturevalue("ingestion_worker")  # starts consuming: stale job first, then the fresh one

    with authed_client.stream("GET", f"/api/chats/{other_chat_id}/messages/{other_assistant_id}/stream") as resp:
        list(resp.iter_lines())
    assert authed_client.get(f"/api/chats/{other_chat_id}/messages").json()[1]["status"] == "complete"

    stale_message = authed_client.get(f"/api/chats/{chat_id}/messages").json()[1]
    assert stale_message["status"] == "failed"
    assert stale_message["content"] == ""


def test_ingestion_worker_discards_job_for_nonexistent_message(authed_client, infra, ingestion_worker) -> None:
    user_id = _decode_user_id(authed_client)
    chat_id = authed_client.post("/api/chats", json={"message": "Hello"}).json()["id"]

    infra["ingestion_job_stream"].publish(
        Job(chat_id=chat_id, user_id=user_id, message_id="does-not-exist", resource_ids=("res-1",))
    )

    # worker must discard the bogus job and keep processing subsequent ones
    resource_id = upload_resource(authed_client, "doc.pdf")
    other_chat_id = authed_client.post(
        "/api/chats", json={"message": f"[doc.pdf](resource://{resource_id})"}
    ).json()["id"]
    other_assistant_id = authed_client.get(f"/api/chats/{other_chat_id}/messages").json()[1]["id"]

    with authed_client.stream("GET", f"/api/chats/{other_chat_id}/messages/{other_assistant_id}/stream") as resp:
        list(resp.iter_lines())
    assert authed_client.get(f"/api/chats/{other_chat_id}/messages").json()[1]["status"] == "complete"


def test_ingestion_worker_fails_message_on_processing_exception(authed_client, infra) -> None:
    resource_id = upload_resource(authed_client, "doc.pdf")
    chat_id = authed_client.post("/api/chats", json={"message": f"[doc.pdf](resource://{resource_id})"}).json()["id"]

    broken_event_stream = MagicMock(spec=EventStream)
    broken_event_stream.write.side_effect = RuntimeError("boom")
    worker = Worker(
        jobs_stream=infra["ingestion_job_stream"], chat_service=_make_service(infra), event_stream=broken_event_stream,
        response_generator=ResourceIngestion(),
    ).start()
    try:
        status = None
        for _ in range(20):
            status = authed_client.get(f"/api/chats/{chat_id}/messages").json()[1]["status"]
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
    chat_id = authed_client.post(
        "/api/chats", json={"message": f"[report.pdf](resource://{resource_id})"},
    ).json()["id"]
    assistant_id = authed_client.get(f"/api/chats/{chat_id}/messages").json()[1]["id"]

    start = time.monotonic()
    with authed_client.stream("GET", f"/api/chats/{chat_id}/messages/{assistant_id}/stream") as resp:
        lines = list(resp.iter_lines())
    elapsed = time.monotonic() - start

    assert "event: error" in lines
    assert elapsed > 0.1  # governed by resource_processing_timeout (0.2s), not message_generation_timeout (0.05s)


def test_generate_answers_question_about_uploaded_document(authed_client, llm_ingestion_worker, llm_worker) -> None:
    pdf_bytes = (DOCUMENTS_DIR / "report.pdf").read_bytes()
    resource_id = authed_client.post(
        "/api/upload-resource", files={"file": ("report.pdf", pdf_bytes, "application/pdf")},
    ).json()["id"]

    chat_id = authed_client.post(
        "/api/chats", json={"message": f"[report.pdf](resource://{resource_id})"},
    ).json()["id"]
    ingestion_assistant_id = authed_client.get(f"/api/chats/{chat_id}/messages").json()[1]["id"]
    with authed_client.stream(
        "GET", f"/api/chats/{chat_id}/messages/{ingestion_assistant_id}/stream"
    ) as resp:
        list(resp.iter_lines())
    assert authed_client.get(f"/api/chats/{chat_id}/messages").json()[1]["status"] == "complete"

    authed_client.post(
        f"/api/chats/{chat_id}/messages",
        json={"content": "What is the launch code mentioned in the document? Reply with just the code, nothing else."},
    )
    answer_id = authed_client.get(f"/api/chats/{chat_id}/messages").json()[3]["id"]
    with authed_client.stream("GET", f"/api/chats/{chat_id}/messages/{answer_id}/stream") as resp:
        list(resp.iter_lines())

    messages = authed_client.get(f"/api/chats/{chat_id}/messages").json()
    assert messages[3]["status"] == "complete"
    assert "bluebird-7" in messages[3]["content"].lower()
