import pytest
from unittest.mock import AsyncMock, MagicMock

from chatops.stream.event_stream import EventStream, StreamEntry
from chatops.stream.message_observer import (
    MessageAlreadyConsumedError,
    MessageGenerationTimeoutError,
    MessageObserver,
)
from chatops.domain.chat import EOM


def make_stream(tokens: list[str], exists: bool = True) -> EventStream:
    stream = MagicMock(spec=EventStream)
    entries = [StreamEntry(id=str(i), data={"token": token}) for i, token in enumerate(tokens)]
    if exists:
        stream.read = AsyncMock(return_value=entries)
    else:
        stream.read = AsyncMock(side_effect=TimeoutError)
    return stream


@pytest.mark.asyncio
async def test_observer_retries_on_stream_timeout_until_generation_timeout() -> None:
    observer = MessageObserver(
        "chat-1", "msg-1", stream=make_stream(["Hi", " there", EOM], exists=False), timeout=0.05
    )
    with pytest.raises(MessageGenerationTimeoutError):
        _ = [e async for e in observer]


@pytest.mark.asyncio
async def test_observer_yields_tokens_excluding_eom() -> None:
    observer = MessageObserver("chat-1", "msg-1", stream=make_stream(["Hi", " there", EOM]))
    events = [e async for e in observer]
    assert "".join(e.token for e in events) == "Hi there"


@pytest.mark.asyncio
async def test_observer_raises_on_second_iteration() -> None:
    observer = MessageObserver("chat-1", "msg-1", stream=make_stream(["Hi", " there", EOM]))
    _ = [e async for e in observer]
    with pytest.raises(MessageAlreadyConsumedError):
        _ = [e async for e in observer]
