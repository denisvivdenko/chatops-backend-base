import base64
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from chatops.domain.chat import Message, MessageRole, MessageStatus
from chatops.services.chat_service import ChatService
from chatops.stream.job_stream import Job
from chatops.workers.llm_message_generator import LLMMessageGenerator

USER_ID = "test-user"
MODEL = "gpt-4o-mini"
IMAGES_DIR = Path(__file__).parent / "fixtures" / "images"


def _image_markdown(filename: str) -> str:
    encoded = base64.b64encode((IMAGES_DIR / filename).read_bytes()).decode()
    return f"![image-name](data:image/png;base64,{encoded})"


@pytest.mark.flaky(reruns=2, reruns_delay=2)
def test_generate_streams_response_using_conversation_history_and_system_prompt(openai_client) -> None:
    chat_service = MagicMock(spec=ChatService)
    chat_service.fetch_messages.return_value = [
        Message(id="msg-1", role=MessageRole.USER, status=MessageStatus.COMPLETE, content="My name is Zork.", created_at=1),
        Message(id="msg-2", role=MessageRole.ASSISTANT, status=MessageStatus.COMPLETE, content="Nice to meet you, Zork!", created_at=2),
        Message(
            id="msg-3", role=MessageRole.USER, status=MessageStatus.COMPLETE,
            content="Count from 1 to 3, one number per line, then say my name. Reply with nothing else.",
            created_at=3,
        ),
        Message(id="msg-4", role=MessageRole.ASSISTANT, status=MessageStatus.PENDING, content="", created_at=4),
    ]

    generator = LLMMessageGenerator(
        chat_service=chat_service, client=openai_client, model=MODEL,
        system_prompt="No matter what you are asked, always end your response with the exact word 'BANANA'.",
    )
    chunks = list(generator.generate(Job(chat_id="chat-1", user_id=USER_ID, message_id="msg-4")))
    result = "".join(chunks)

    assert len(chunks) > 1  # streamed, not returned as one blob
    assert "1" in result and "3" in result
    assert "zork" in result.lower()  # recalled from conversation history
    assert "banana" in result.lower()  # system prompt applied


@pytest.mark.flaky(reruns=2, reruns_delay=2)
@pytest.mark.parametrize("order", [
    ("circle", "triangle", "rectangle"),
    ("triangle", "rectangle", "circle")
], ids=["circle-triangle-rectangle", "triangle-rectangle-circle"])
def test_generate_sees_images_submitted_across_messages_in_order(openai_client, order) -> None:
    first, second, third = order
    chat_service = MagicMock(spec=ChatService)
    chat_service.fetch_messages.return_value = [
        Message(
            id="msg-1", role=MessageRole.USER, status=MessageStatus.COMPLETE,
            content=f"{_image_markdown(f'{first}.png')}\nHere is an image.", created_at=1,
        ),
        Message(id="msg-2", role=MessageRole.ASSISTANT, status=MessageStatus.COMPLETE, content="Got it.", created_at=2),
        Message(
            id="msg-3", role=MessageRole.USER, status=MessageStatus.COMPLETE,
            content=f"{_image_markdown(f'{second}.png')}\nHere is another image.", created_at=3,
        ),
        Message(id="msg-4", role=MessageRole.ASSISTANT, status=MessageStatus.COMPLETE, content="Got it.", created_at=4),
        Message(
            id="msg-5", role=MessageRole.USER, status=MessageStatus.COMPLETE,
            content=f"{_image_markdown(f'{third}.png')}\nHere is another image.", created_at=5,
        ),
        Message(id="msg-6", role=MessageRole.ASSISTANT, status=MessageStatus.COMPLETE, content="Got it.", created_at=6),
        Message(
            id="msg-7", role=MessageRole.USER, status=MessageStatus.COMPLETE,
            content=(
                "List, in the order I showed them, the shapes drawn in the three images I sent you. "
                "Reply with just the three shape names separated by commas, nothing else."
            ),
            created_at=7,
        ),
        Message(id="msg-8", role=MessageRole.ASSISTANT, status=MessageStatus.PENDING, content="", created_at=8),
    ]

    generator = LLMMessageGenerator(chat_service=chat_service, client=openai_client, model=MODEL)
    result = "".join(generator.generate(Job(chat_id="chat-1", user_id=USER_ID, message_id="msg-8"))).lower()

    assert first in result and second in result and third in result
    assert result.index(first) < result.index(second) < result.index(third)
