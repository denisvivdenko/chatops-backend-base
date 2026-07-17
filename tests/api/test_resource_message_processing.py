import time

import pytest

from chatops.workers.ingestion_worker import DOCUMENT_PROCESSED_RESPONSE

PDF_CONTENT = b"%PDF-1.4\n%mock pdf content"


def _upload_resource(client) -> str:
    response = client.post(
        "/api/upload-resource", files={"file": ("report.pdf", PDF_CONTENT, "application/pdf")},
    )
    return response.json()["id"]


def test_create_chat_with_resource_ref_is_processed_by_ingestion_worker(authed_client_with_ingestion_worker):
    resource_id = _upload_resource(authed_client_with_ingestion_worker)

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


def test_follow_up_message_with_resource_ref_is_processed_by_ingestion_worker(authed_client_with_ingestion_worker):
    resource_id = _upload_resource(authed_client_with_ingestion_worker)

    chat_id = authed_client_with_ingestion_worker.post(
        "/api/chats", json={"message": f"[report.pdf](resource://{resource_id})"},
    ).json()["id"]
    first_assistant_id = authed_client_with_ingestion_worker.get(f"/api/chats/{chat_id}/messages").json()[1]["id"]
    with authed_client_with_ingestion_worker.stream(
        "GET", f"/api/chats/{chat_id}/messages/{first_assistant_id}/stream"
    ) as resp:
        list(resp.iter_lines())

    authed_client_with_ingestion_worker.post(
        f"/api/chats/{chat_id}/messages", json={"content": f"[report.pdf](resource://{resource_id})"},
    )
    second_assistant_id = authed_client_with_ingestion_worker.get(f"/api/chats/{chat_id}/messages").json()[3]["id"]

    with authed_client_with_ingestion_worker.stream(
        "GET", f"/api/chats/{chat_id}/messages/{second_assistant_id}/stream"
    ) as resp:
        list(resp.iter_lines())

    messages = authed_client_with_ingestion_worker.get(f"/api/chats/{chat_id}/messages").json()
    assert messages[3]["id"] == second_assistant_id
    assert messages[3]["status"] == "complete"
    assert messages[3]["content"] == DOCUMENT_PROCESSED_RESPONSE


@pytest.mark.parametrize(
    "settings",
    [{"message_generation_timeout": 0.05}],
    indirect=True,
)
def test_retry_of_failed_resource_ref_message_is_processed_by_ingestion_worker(authed_client, infra, request):
    resource_id = _upload_resource(authed_client)

    chat_id = authed_client.post(
        "/api/chats", json={"message": f"[report.pdf](resource://{resource_id})"},
    ).json()["id"]
    assistant_id = authed_client.get(f"/api/chats/{chat_id}/messages").json()[1]["id"]

    time.sleep(0.1)  # no worker running yet, so the message times out and fails
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


def test_modify_message_adding_resource_ref_is_processed_by_ingestion_worker(authed_client_with_ingestion_worker):
    resource_id = _upload_resource(authed_client_with_ingestion_worker)

    chat_id = authed_client_with_ingestion_worker.post(
        "/api/chats", json={"message": f"[report.pdf](resource://{resource_id})"},
    ).json()["id"]
    user_message_id = authed_client_with_ingestion_worker.get(f"/api/chats/{chat_id}/messages").json()[0]["id"]
    first_assistant_id = authed_client_with_ingestion_worker.get(f"/api/chats/{chat_id}/messages").json()[1]["id"]
    with authed_client_with_ingestion_worker.stream(
        "GET", f"/api/chats/{chat_id}/messages/{first_assistant_id}/stream"
    ) as resp:
        list(resp.iter_lines())

    modify_response = authed_client_with_ingestion_worker.post(
        f"/api/chats/{chat_id}/messages/{user_message_id}/modify",
        json={"content": f"Please check [report.pdf](resource://{resource_id}) again"},
    )
    assert modify_response.status_code == 200
    new_assistant_id = modify_response.json()["id"]

    with authed_client_with_ingestion_worker.stream(
        "GET", f"/api/chats/{chat_id}/messages/{new_assistant_id}/stream"
    ) as resp:
        list(resp.iter_lines())

    messages = authed_client_with_ingestion_worker.get(f"/api/chats/{chat_id}/messages").json()
    assert messages[1]["id"] == new_assistant_id
    assert messages[1]["status"] == "complete"
    assert messages[1]["content"] == DOCUMENT_PROCESSED_RESPONSE
