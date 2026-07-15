# Plan: Document resource endpoints

Implementation plan for `resources-backend-ticket.md`, with the ticket's open
questions and design gaps resolved below. Written to be worked in TDD order —
each component section lists the files to touch and the behaviors to write
tests against first.

## Resolved decisions

| Question | Decision | Why |
|---|---|---|
| What does "ingest/preprocess" do? | No-op stub: verify ownership, mark message `complete`, canned reply content. No PDF parsing, no chunking/embeddings. | The existing assistant-reply worker has no LLM call at all yet (`worker.py` streams a hardcoded string) — there's no consumer for parsed/embedded content to feed. Building real ingestion now would be unused work. This still fulfills the ticket's contract (ownership check is real, status transitions are real). |
| Reject bad `resource://` refs sync or async? | Synchronous 4xx from `POST /chats/{chatId}/messages`, before the pending message is created. | Matches every existing validation in `ChatService` (`_assert_owns_chat` etc.) — all raise synchronously, mapped to 4xx in the router. No precedent for deferring validation into the worker. |
| Batch limit on refs per message? | None. | No stated frontend constraint, and no real per-document cost yet (no-op ingestion). Arbitrary caps can be added once real ingestion has a cost model. |
| Which worker processes ingestion? | A dedicated `IngestionWorker` on its own Redis queue, not the existing `AssistantJob`/`Worker`. | User decision — keep the two kinds of background work on separate queues/workers rather than overloading the existing job type. |
| Does a resourced message also get a normal assistant job? | No — exclusive routing. Refs present → `IngestionJob` only. No refs → `AssistantJob` only, unchanged. | Ticket: "represent that ingestion as the ordinary assistant reply for this message" — one pending message, one worker owns completing it. Publishing to both would race two workers calling `complete_message` on the same `message_id`, and `ALLOWED_MESSAGE_STATUS_TRANSITIONS` only allows `pending → {complete, failed}` once. |
| Per-chat "already ingested" tracking? | None needed. | Consequence of the no-op decision: every message with a resource ref triggers a fresh ingestion job regardless of prior chats, so the ticket's "re-ingest per chat, don't treat as already-done" requirement is trivially satisfied without extra state. |
| Where do uploaded PDF bytes live? | Local disk volume, mounted on the `api` container only. | User decision. Since ingestion doesn't parse bytes yet, nothing server-side reads the file back — only the upload path (`api`) needs the volume. `worker` and `ingestion-worker` don't. |
| PDF type validation | Sniff the magic bytes (`%PDF-` header) — do not trust the multipart field's declared `Content-Type`. | Ticket explicitly says "not just trusted from the client"; `Content-Type` is client-supplied and trivially spoofable. |
| 20MB size limit enforcement | Read the full upload, then check `len(content)`. | Matches this codebase's existing simplicity level; streaming cutoff would defend against a DoS scenario the ticket doesn't ask for and no other endpoint here guards against either. |
| SSE completion (discovered while reading code, not a judgment call) | `IngestionWorker` must write at least one `token` event plus a final `token: EOM` event to the `EventStream`, same as `Worker._process` does, before/around calling `complete_message`. | `MessageObserver._iterate` (`stream/message_observer.py:41-56`) only stops reading when it sees `EOM`. If `IngestionWorker` only calls `complete_message` without writing to the stream, the frontend's `GET /chats/{chatId}/messages/{messageId}/stream` call hangs until `message_generation_timeout`. |

## Data model

New file `src/chatops/domain/resource.py`:

```python
from pydantic import BaseModel

class Resource(BaseModel):
    id: str
    user_id: str
    filename: str
    file_path: str
    created_at: int
```

## Component plan

### 1. `ResourceRepository`

New file `src/chatops/repositories/resource_repository.py`, mirroring
`chat_repository.py`'s ABC + Mongo-impl shape.

```python
class ResourceRepository(ABC):
    def save_resource(self, resource: Resource) -> None: ...
    def fetch_resource(self, resource_id: str) -> Resource: ...  # raises KeyError if missing
    def fetch_resources(self, user_id: str) -> list[Resource]: ...  # sorted by created_at DESCENDING

class MongoResourceRepository(ResourceRepository):
    # collection "resources", indexed on user_id
```

**Tests first** (`tests/test_resource_repository.py` or reuse `conftest.py` mongo fixture pattern):
- save + fetch round-trip
- `fetch_resource` on unknown id raises `KeyError`
- `fetch_resources` returns only the calling user's resources, newest first

### 2. File storage helper

New file `src/chatops/storage/resource_storage.py` (new `storage` package). Simple wrapper, not a repository:

```python
class ResourceStorage:
    def __init__(self, base_dir: Path) -> None: ...
    def save(self, resource_id: str, content: bytes) -> str:  # returns file_path
    def read(self, file_path: str) -> bytes  # unused today, but needed for future real ingestion
```

New setting in `src/chatops/settings.py`: `resource_storage_dir: str = "/data/resources"`.

**Tests first**: save writes a file at the expected path; read round-trips bytes.

### 3. `POST /api/upload-resource`

Add to `src/chatops/api/main.py` (or split into a new router if it grows —
match existing single-router style for now). New `ResourceService` in
`src/chatops/services/resource_service.py` handling validation + persistence,
analogous to `ChatService`.

Behavior:
1. Read the single `file` multipart field fully into memory.
2. Reject (400, `{"error": "invalid_file_type"}`) if content doesn't start with `b"%PDF-"`.
3. Reject (400, `{"error": "file_too_large"}`) if `len(content) > 20 * 1024 * 1024`.
4. Generate `id = uuid4()`, write bytes via `ResourceStorage.save`, save `Resource` (id, user_id from `CurrentUserIdDep`, filename from the client-provided filename, file_path, created_at) via `ResourceRepository`.
5. Return `201 {"id": ..., "filename": ...}`.

**Tests first** (`tests/api/test_upload_resource.py`):
- valid PDF upload → 2xx, response has `id` + `filename`
- non-PDF content (even with `Content-Type: application/pdf` claimed) → 4xx, not 500
- oversized file (>20MB) → 4xx, not 500
- unauthenticated request → 401
- two different users uploading same filename don't collide (different `id`s)

### 4. `GET /api/resources`

Behavior: return all resources owned by `CurrentUserIdDep`, most-recent-first, as `[{"id", "filename"}, ...]`.

**Tests first** (`tests/api/test_list_resources.py`):
- empty list for a user with no uploads
- returns only the calling user's resources, not another user's
- ordering is most-recently-uploaded first
- unauthenticated request → 401

### 5. Resource-ref parsing

New pure function, `src/chatops/services/resource_refs.py`:

```python
def parse_resource_refs(content: str) -> list[str]:
    """Extract every artifactId from [filename](resource://<artifactId>) links in content."""
```

**Tests first** (`tests/test_resource_refs.py`):
- no refs → `[]`
- single ref → `["id1"]`
- multiple refs → all extracted, in order, duplicates preserved as-is (no dedup — not asked for)
- ref embedded alongside plain text
- malformed/near-miss patterns (e.g. missing scheme, missing closing paren) are ignored

### 6. `IngestionJob` + `IngestionJobStream`

New file `src/chatops/stream/ingestion_job_stream.py`, mirroring `job_stream.py`:

```python
class IngestionJob(NamedTuple):
    chat_id: str
    user_id: str
    message_id: str
    resource_ids: tuple[str, ...]

class IngestionJobStream(ABC):
    def publish(self, job: IngestionJob) -> None: ...
    def consume(self) -> IngestionJob: ...  # raises TimeoutError

class RedisIngestionJobStream(IngestionJobStream):
    # separate Redis key, e.g. REDIS_INGESTION_JOBS_KEY = "ingestion_jobs"
```

**Tests first**: publish then consume round-trips fields; consume raises `TimeoutError` when empty (mirror `tests/test_event_stream.py` / existing job-stream tests if any exist — check `tests/` for a `test_job_stream.py` equivalent first).

### 7. Extend `ChatService.send_message`

Modify `src/chatops/services/chat_service.py`. New exceptions:

```python
class ResourceNotFoundError(Exception): pass
class ResourceAccessDeniedError(Exception): pass
```

`ChatService` gains a `resource_repository: ResourceRepository` constructor dependency. `send_message` gains an `ingestion_jobs: IngestionJobStream` param (or route selection happens one level up in the API layer — decide during implementation, whichever keeps `send_message`'s signature closest to today's).

```python
def send_message(self, chat_id, user_id, content, jobs_stream, ingestion_jobs):
    self._assert_owns_chat(chat_id, user_id)
    resource_ids = parse_resource_refs(content)
    for rid in resource_ids:
        self._assert_owns_resource(rid, user_id)  # raises ResourceNotFoundError / ResourceAccessDeniedError
    ...  # existing pending/message creation unchanged
    if resource_ids:
        ingestion_jobs.publish(IngestionJob(chat_id, user_id, assistant_message.id, tuple(resource_ids)))
    else:
        jobs_stream.publish(AssistantJob(chat_id=chat_id, user_id=user_id, message_id=assistant_message.id))
    return assistant_message
```

Router (`main.py`) `send_message` gains `except ResourceNotFoundError: 404` / `except ResourceAccessDeniedError: 403`, same pattern as existing chat-ownership errors.

**Tests first** (extend `tests/test_chat_service.py` + `tests/api/test_messages.py`):
- message with no resource refs behaves exactly as today (published to `AssistantJob` queue, unchanged)
- message with a valid, owned resource ref publishes to `IngestionJob` queue instead, not `AssistantJob`
- message with a ref to a nonexistent resource → `ResourceNotFoundError` / 404, no pending message created, nothing published to either queue
- message with a ref to another user's resource → `ResourceAccessDeniedError` / 403, no pending message created
- message mixing a valid ref with plain text still routes to ingestion (per ticket: content is opaque markdown, scan for the pattern regardless of surrounding text)
- multiple valid refs in one message → single `IngestionJob` with all `resource_ids`

### 8. `IngestionWorker`

New file `src/chatops/workers/ingestion_worker.py`, mirroring `workers/worker.py`'s structure (`start`/`stop`/`_run`/`_process`, `if __name__ == "__main__"` entrypoint).

```python
def _process(self, job: IngestionJob) -> None:
    # same guard pattern as Worker._process: fetch message, discard if not found / not PENDING
    try:
        stream_key = self._event_stream.stream_key(job.chat_id, job.message_id)
        response = "Document processed."
        self._event_stream.write(stream_key, {"token": response})
        self._service.complete_message(job.chat_id, job.user_id, job.message_id, response)
        self._event_stream.write(stream_key, {"token": EOM})
    except Exception:
        self._service.fail_message(job.chat_id, job.user_id, job.message_id)
```

No file reads from `ResourceStorage` needed yet (no-op ingestion) — ownership was already verified synchronously in `send_message` before the job was published, so `_process` doesn't need to re-check it.

**Tests first** (`tests/test_ingestion_worker.py`, mirror whatever exists testing `Worker`):
- valid job → message transitions to `complete`, event stream receives token(s) + `EOM`
- job for a message that's no longer `PENDING` → discarded, no-op (mirrors existing `Worker._process` guard)
- job for a nonexistent message → discarded, no-op
- exception during processing → `fail_message` called

### 9. Wiring

`src/chatops/api/dependencies.py`:
- `get_resource_repository()`, `get_resource_storage()`, `get_ingestion_job_stream()`
- `get_resource_service(...)`
- `ResourceServiceDep`, `ResourceRepositoryDep` (if needed directly), `IngestionJobStreamDep`
- `get_chat_service` gains the new `resource_repository` dependency

`src/chatops/settings.py`: add `resource_storage_dir: str = "/data/resources"`.

### 10. `docker-compose.yml`

- New volume, e.g. `resource-data`.
- `api` service: mount `resource-data:/data/resources`.
- New `ingestion-worker` service, mirroring `worker`: `command: python -m chatops.workers.ingestion_worker`, same `REDIS_HOST`/`MONGO_HOST` env, same `depends_on`, profile `[deploy]`. No volume mount (doesn't touch file bytes).

## Out of scope (carried from ticket)

- `DELETE` for resources.
- Automatic retry of failed uploads.
- Dedup of repeated uploads of identical content.
- Any real PDF parsing/chunking/embeddings/vector store — deferred until an actual LLM integration exists to consume it.

## Suggested TDD order

1. `parse_resource_refs` (pure function, no I/O)
2. `ResourceRepository` (Mongo)
3. `ResourceStorage` (disk)
4. `ResourceService` + `POST /upload-resource` + `GET /resources`
5. `IngestionJobStream`
6. `ChatService.send_message` extension (ownership validation + routing)
7. `IngestionWorker`
8. `docker-compose.yml` + manual end-to-end check
