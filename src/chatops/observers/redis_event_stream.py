from chatops.observers.event_stream import EventStream, MessageToken


class RedisEventStream(EventStream):
    async def exists(self, chat_id: str, message_id: str) -> bool:
        raise NotImplementedError

    async def listen_for_message_tokens(self, chat_id: str, message_id: str, from_seq_id: int) -> list[MessageToken]:
        raise NotImplementedError
