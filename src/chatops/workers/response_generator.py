import time
from abc import ABC, abstractmethod
from typing import Iterator

from chatops.stream.job_stream import Job

HARDCODED_RESPONSE = """
## Markdown support

This response demonstrates **bold**, *italic*, and `inline code`.

### Lists

- Unordered items work fine
- As do nested concepts

1. Ordered lists too
2. With multiple entries

### Code blocks

```ts
function greet(name: string): string {
  return `Hello, ${name}!`;
}
```

> Blockquotes are also supported for callouts or citations.

---

Let me know what you'd like to explore next.`;
"""

TEST_RESPONSE = HARDCODED_RESPONSE[:12]

DOCUMENT_PROCESSED_RESPONSE = "Document processed"


class ResponseGenerator(ABC):
    @abstractmethod
    def generate(self, job: Job) -> Iterator[str]:
        """Yields response chunks for the given job, in order.
        The concatenation of all yielded chunks is the full response content."""
        ...


class MessageGeneration(ResponseGenerator):
    def __init__(self, response: str = HARDCODED_RESPONSE, chunk_size: int = 6, delay: float = 0.1) -> None:
        self._response = response
        self._chunk_size = chunk_size
        self._delay = delay

    def generate(self, job: Job) -> Iterator[str]:
        for i in range(0, len(self._response), self._chunk_size):
            yield self._response[i:i + self._chunk_size]
            time.sleep(self._delay)


class ResourceIngestion(ResponseGenerator):
    def __init__(self, response: str = DOCUMENT_PROCESSED_RESPONSE, processing_delay: float = 0.0) -> None:
        self._response = response
        self._processing_delay = processing_delay

    def generate(self, job: Job) -> Iterator[str]:
        if self._processing_delay:
            time.sleep(self._processing_delay)
        yield self._response
