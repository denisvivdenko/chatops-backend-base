import pytest
from unittest.mock import AsyncMock, MagicMock

from chatops.observers.event_stream import EventStream, StreamEntry, StreamNotFoundError
from chatops.observers.message_observer import MessageObserver, MessageNotObservableError, MessageIsAlreadyConsumed
from chatops.domain.chat import EOM


def make_stream(tokens: list[str], exists: bool = True) -> EventStream:
    stream = MagicMock(spec=EventStream)
    entries = [StreamEntry(id=str(i), data={"token": token}) for i, token in enumerate(tokens)]
    if exists:
        stream.read = AsyncMock(return_value=entries)
    else:
        stream.read = AsyncMock(side_effect=StreamNotFoundError)
    return stream


@pytest.mark.asyncio
async def test_observer_raises_when_stream_does_not_exist() -> None:
    observer = MessageObserver("chat-1", "msg-1", stream=make_stream(["Hi", " there", EOM], exists=False))
    with pytest.raises(MessageNotObservableError):
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
    with pytest.raises(MessageIsAlreadyConsumed):
        _ = [e async for e in observer]
