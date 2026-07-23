from chatops.services.chat_service import ChatService
from chatops.services.resource_service import ResourceService
from chatops.domain.chat import MessageStatus

USER_ID = "test-user"


def _make_service(infra) -> ChatService:
    resource_service = ResourceService(resource_repository=infra["resource_repo"], resource_storage=infra["resource_storage"])
    return ChatService(chat_repository=infra["repo"], resource_service=resource_service)


def test_fail_message_marks_assistant_message_as_failed(infra) -> None:
    service = _make_service(infra)
    chat = service.create_chat("Hello", USER_ID, infra["job_stream"], infra["ingestion_job_stream"])
    assistant = service.fetch_messages(chat.id, USER_ID)[1]

    service.fail_message(chat.id, USER_ID, assistant.id)

    failed = service.fetch_messages(chat.id, USER_ID)[1]
    assert failed.id == assistant.id
    assert failed.status == MessageStatus.FAILED
