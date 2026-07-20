from typing import Iterator

import pytest
from openai import OpenAI

from chatops.services.chat_service import ChatService
from chatops.services.resource_service import ResourceService
from chatops.settings import Settings
from chatops.stream.job_stream import Job
from chatops.workers.llm_message_generator import LLMMessageGenerator
from chatops.workers.worker import Worker

USER_ID = "test-user"
MODEL = "gpt-4o-mini"


def _make_service(infra) -> ChatService:
    resource_service = ResourceService(resource_repository=infra["resource_repo"], resource_storage=infra["resource_storage"])
    return ChatService(chat_repository=infra["repo"], resource_service=resource_service)


@pytest.fixture
def openai_client(settings: Settings) -> OpenAI:
    if not settings.openai_api_key:
        pytest.skip("OPENAI_API_KEY not set")
    return OpenAI(api_key=settings.openai_api_key)


def test_generate_streams_response_in_multiple_chunks(infra, openai_client) -> None:
    service = _make_service(infra)
    jobs_stream, ingestion_jobs = infra["job_stream"], infra["ingestion_job_stream"]
    chat = service.create_chat("Count from 1 to 10, one number per line, nothing else.", USER_ID, jobs_stream, ingestion_jobs)
    assistant = service.fetch_messages(chat.id, USER_ID)[1]

    generator = LLMMessageGenerator(chat_service=service, client=openai_client, model=MODEL)
    chunks = list(generator.generate(Job(chat_id=chat.id, user_id=USER_ID, message_id=assistant.id)))

    assert len(chunks) > 1
    result = "".join(chunks)
    assert "1" in result and "10" in result


def test_generate_uses_conversation_history_for_context(infra, openai_client) -> None:
    service = _make_service(infra)
    jobs_stream, ingestion_jobs = infra["job_stream"], infra["ingestion_job_stream"]

    chat = service.create_chat("My name is Zork.", USER_ID, jobs_stream, ingestion_jobs)
    first_reply = service.fetch_messages(chat.id, USER_ID)[1]
    service.complete_message(chat.id, USER_ID, first_reply.id, "Nice to meet you, Zork!")
    assistant = service.send_message(
        chat.id, USER_ID, "What is my name? Reply with just the name, nothing else.", jobs_stream, ingestion_jobs,
    )

    generator = LLMMessageGenerator(chat_service=service, client=openai_client, model=MODEL)
    result = "".join(generator.generate(Job(chat_id=chat.id, user_id=USER_ID, message_id=assistant.id)))

    assert "zork" in result.lower()


def test_generate_applies_system_prompt(infra, openai_client) -> None:
    service = _make_service(infra)
    jobs_stream, ingestion_jobs = infra["job_stream"], infra["ingestion_job_stream"]
    chat = service.create_chat("Say hello.", USER_ID, jobs_stream, ingestion_jobs)
    assistant = service.fetch_messages(chat.id, USER_ID)[1]

    generator = LLMMessageGenerator(
        chat_service=service, client=openai_client, model=MODEL,
        system_prompt="No matter what you are asked, always end your response with the exact word 'BANANA'.",
    )
    result = "".join(generator.generate(Job(chat_id=chat.id, user_id=USER_ID, message_id=assistant.id)))

    assert "banana" in result.lower()


@pytest.fixture
def llm_worker(infra, openai_client) -> Iterator[Worker]:
    chat_service = _make_service(infra)
    w = Worker(
        jobs_stream=infra["job_stream"],
        chat_service=chat_service,
        event_stream=infra["event_stream"],
        response_generator=LLMMessageGenerator(chat_service=chat_service, client=openai_client, model=MODEL),
    ).start()
    yield w
    w.stop()


def test_message_generated_via_llm_worker_completes_end_to_end(authed_client, llm_worker) -> None:
    chat_id = authed_client.post("/api/chats", json={"message": "Reply with exactly one word: PONG"}).json()["id"]
    assistant_id = authed_client.get(f"/api/chats/{chat_id}/messages").json()[1]["id"]

    with authed_client.stream("GET", f"/api/chats/{chat_id}/messages/{assistant_id}/stream") as resp:
        list(resp.iter_lines())

    messages = authed_client.get(f"/api/chats/{chat_id}/messages").json()
    assert messages[1]["status"] == "complete"
    assert "pong" in messages[1]["content"].lower()
