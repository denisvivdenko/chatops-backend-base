from typing import Iterator

from openai import OpenAI

from chatops.domain.chat import MessageRole
from chatops.services.chat_service import ChatService
from chatops.stream.job_stream import Job
from chatops.workers.response_generator import ResponseGenerator

_ROLE_MAP = {
    MessageRole.USER: "user",
    MessageRole.ASSISTANT: "assistant",
}


class LLMMessageGenerator(ResponseGenerator):
    def __init__(
        self,
        chat_service: ChatService,
        client: OpenAI,
        model: str = "gpt-4o-mini",
        system_prompt: str | None = None,
    ) -> None:
        self._service = chat_service
        self._client = client
        self._model = model
        self._system_prompt = system_prompt

    def generate(self, job: Job) -> Iterator[str]:
        history = self._service.fetch_messages(job.chat_id, job.user_id)
        messages = [
            {"role": _ROLE_MAP[message.role], "content": message.content}
            for message in history
            if message.id != job.message_id
        ]
        if self._system_prompt is not None:
            messages.insert(0, {"role": "system", "content": self._system_prompt})

        stream = self._client.chat.completions.create(model=self._model, messages=messages, stream=True)
        for chunk in stream:
            delta = chunk.choices[0].delta.content
            if delta:
                yield delta
