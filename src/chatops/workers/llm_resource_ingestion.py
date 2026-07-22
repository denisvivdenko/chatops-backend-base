import base64
from typing import Iterator

from openai import OpenAI

from chatops.services.resource_service import ResourceService
from chatops.stream.job_stream import Job
from chatops.workers.response_generator import ResponseGenerator

_PROMPT = (
    "Convert the content of the attached document into clean Markdown, preserving headings and "
    "paragraph text. Reply with only the Markdown, nothing else."
)


class LLMResourceIngestion(ResponseGenerator):
    def __init__(self, resource_service: ResourceService, client: OpenAI, model: str = "gpt-4o-mini") -> None:
        self._resource_service = resource_service
        self._client = client
        self._model = model

    def generate(self, job: Job) -> Iterator[str]:
        documents = [self._to_markdown(resource_id, job.user_id) for resource_id in job.resource_ids]
        yield "\n\n---\n\n".join(documents)

    def _to_markdown(self, resource_id: str, user_id: str) -> str:
        resource = self._resource_service.get_resource(resource_id, user_id)
        content = self._resource_service.read_resource_content(resource)
        file_data = f"data:application/pdf;base64,{base64.b64encode(content).decode()}"

        response = self._client.chat.completions.create(
            model=self._model,
            messages=[{
                "role": "user",
                "content": [
                    {"type": "text", "text": _PROMPT},
                    {"type": "file", "file": {"filename": resource.filename, "file_data": file_data}},
                ],
            }],
        )
        return response.choices[0].message.content
