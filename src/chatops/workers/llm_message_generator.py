import re
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

_IMAGE_PATTERN = re.compile(r"!\[[^\]]*\]\((data:image/[^;]+;base64,[^)]+)\)")


def _parse_content(content: str) -> str | list[dict]:
    parts = _IMAGE_PATTERN.split(content)
    if len(parts) == 1:
        return content

    blocks: list[dict] = []
    for index, part in enumerate(parts):
        if index % 2 == 0:
            text = part.strip()
            if text:
                blocks.append({"type": "text", "text": text})
        else:
            blocks.append({"type": "image_url", "image_url": {"url": part}})
    return blocks


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
            {"role": _ROLE_MAP[message.role], "content": _parse_content(message.content)}
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
