import uuid
import time

import pytest

from chatops.services.chat_service import ChatService, MessageNotAssistantError
from chatops.domain.chat import Message, MessageRole, MessageStatus
from chatops.settings import MessageTimeoutSettings


def _make_message(
    role: MessageRole = MessageRole.ASSISTANT,
    resource_ids: list[str] | None = None,
    created_at: int | None = None,
) -> Message:
    return Message(
        id=str(uuid.uuid4()),
        role=role,
        status=MessageStatus.PENDING,
        content="",
        created_at=created_at if created_at is not None else int(time.time() * 1000),
        resource_ids_to_process=resource_ids or [],
    )


def test_estimate_message_timeout_raises_for_non_assistant_message() -> None:
    message = _make_message(role=MessageRole.USER)

    with pytest.raises(MessageNotAssistantError):
        ChatService.estimate_message_timeout(message, MessageTimeoutSettings())


def test_estimate_message_timeout_returns_generation_timeout_for_message_without_resources() -> None:
    timeout_settings = MessageTimeoutSettings(message_generation_timeout=10, resource_processing_timeout=20)
    message = _make_message()

    result = ChatService.estimate_message_timeout(message, timeout_settings)

    assert result == pytest.approx(10, abs=0.5)


def test_estimate_message_timeout_multiplies_resource_timeout_by_resource_count() -> None:
    timeout_settings = MessageTimeoutSettings(message_generation_timeout=10, resource_processing_timeout=20)
    message = _make_message(resource_ids=["resource-1", "resource-2", "resource-3"])

    result = ChatService.estimate_message_timeout(message, timeout_settings)

    assert result == pytest.approx(60, abs=0.5)


def test_estimate_message_timeout_subtracts_elapsed_time_since_created_at() -> None:
    timeout_settings = MessageTimeoutSettings(message_generation_timeout=10, resource_processing_timeout=20)
    created_at = int(time.time() * 1000) - 4000
    message = _make_message(created_at=created_at)

    result = ChatService.estimate_message_timeout(message, timeout_settings)

    assert result == pytest.approx(6, abs=0.5)


def test_estimate_message_timeout_clamps_at_zero_when_elapsed_exceeds_timeout() -> None:
    timeout_settings = MessageTimeoutSettings(message_generation_timeout=10, resource_processing_timeout=20)
    created_at = int(time.time() * 1000) - 15000
    message = _make_message(created_at=created_at)

    result = ChatService.estimate_message_timeout(message, timeout_settings)

    assert result == 0
