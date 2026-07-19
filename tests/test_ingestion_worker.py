import time
from typing import Iterator
from unittest.mock import MagicMock

import jwt
import pytest

from chatops.services.chat_service import ChatService
from chatops.services.resource_service import ResourceService
from chatops.stream.event_stream import EventStream
from chatops.stream.ingestion_job_stream import IngestionJob
from chatops.workers.ingestion_worker import IngestionWorker

from conftest import sleep_until_message_timed_out

PDF_CONTENT = b"%PDF-1.4\n%mock pdf content"


def _make_service(infra) -> ChatService:
    resource_service = ResourceService(resource_repository=infra["resource_repo"], resource_storage=infra["resource_storage"])
    return ChatService(chat_repository=infra["repo"], resource_service=resource_service)


def _decode_user_id(authed_client) -> str:
    token = authed_client.headers["Authorization"].removeprefix("Bearer ")
    return jwt.decode(token, options={"verify_signature": False})["sub"]


def _upload_resource(authed_client, filename: str) -> str:
    return authed_client.post(
        "/api/upload-resource", files={"file": (filename, PDF_CONTENT, "application/pdf")}
    ).json()["id"]


@pytest.fixture
def ingestion_worker(infra) -> Iterator[IngestionWorker]:
    w = IngestionWorker(
        ingestion_jobs=infra["ingestion_job_stream"],
        chat_service=_make_service(infra),
        event_stream=infra["event_stream"],
    ).start()
    yield w
    w.stop()


def test_message_with_resource_ref_completes_via_ingestion_worker(authed_client, ingestion_worker) -> None:
    resource_id = _upload_resource(authed_client, "doc.pdf")

    chat_id = authed_client.post("/api/chats", json={"message": f"[doc.pdf](resource://{resource_id})"}).json()["id"]
    assistant_id = authed_client.get(f"/api/chats/{chat_id}/messages").json()[1]["id"]

    with authed_client.stream("GET", f"/api/chats/{chat_id}/messages/{assistant_id}/stream") as resp:
        list(resp.iter_lines())

    messages = authed_client.get(f"/api/chats/{chat_id}/messages").json()
    assert messages[1]["status"] == "complete"
    assert messages[1]["content"]


@pytest.mark.parametrize("settings", [{"message_timeout": {"resource_processing_timeout": 0.05}}], indirect=True)
def test_ingestion_worker_discards_stale_job_for_message_already_failed_by_timeout(authed_client, settings, request) -> None:
    resource_id = _upload_resource(authed_client, "doc.pdf")
    chat_id = authed_client.post("/api/chats", json={"message": f"[doc.pdf](resource://{resource_id})"}).json()["id"]
    assistant_message = authed_client.get(f"/api/chats/{chat_id}/messages").json()[1]

    sleep_until_message_timed_out(assistant_message, settings.message_timeout)  # no ingestion worker running yet, so this message's job sits stale in the queue
    assert authed_client.get(f"/api/chats/{chat_id}/messages").json()[1]["status"] == "failed"

    other_resource_id = _upload_resource(authed_client, "other.pdf")
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
        IngestionJob(chat_id=chat_id, user_id=user_id, message_id="does-not-exist", resource_ids=("res-1",))
    )

    # worker must discard the bogus job and keep processing subsequent ones
    resource_id = _upload_resource(authed_client, "doc.pdf")
    other_chat_id = authed_client.post(
        "/api/chats", json={"message": f"[doc.pdf](resource://{resource_id})"}
    ).json()["id"]
    other_assistant_id = authed_client.get(f"/api/chats/{other_chat_id}/messages").json()[1]["id"]

    with authed_client.stream("GET", f"/api/chats/{other_chat_id}/messages/{other_assistant_id}/stream") as resp:
        list(resp.iter_lines())
    assert authed_client.get(f"/api/chats/{other_chat_id}/messages").json()[1]["status"] == "complete"


def test_ingestion_worker_fails_message_on_processing_exception(authed_client, infra) -> None:
    resource_id = _upload_resource(authed_client, "doc.pdf")
    chat_id = authed_client.post("/api/chats", json={"message": f"[doc.pdf](resource://{resource_id})"}).json()["id"]

    broken_event_stream = MagicMock(spec=EventStream)
    broken_event_stream.write.side_effect = RuntimeError("boom")
    worker = IngestionWorker(
        ingestion_jobs=infra["ingestion_job_stream"], chat_service=_make_service(infra), event_stream=broken_event_stream,
    ).start()
    try:
        time.sleep(0.3)
    finally:
        worker.stop()

    assert authed_client.get(f"/api/chats/{chat_id}/messages").json()[1]["status"] == "failed"
