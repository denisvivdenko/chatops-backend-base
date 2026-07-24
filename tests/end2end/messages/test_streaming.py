import json
import pytest

from chatops.workers.response_generator import TEST_RESPONSE
from conftest import sleep_until_message_timed_out


def test_stream_assistant_response_and_emits_done_event_on_completion(authed_client_with_worker):

    def _collect_events(lines, raw, limit=None):
        events = []
        prev = None
        for line in lines:
            if line:
                raw.append(line)
            if line.startswith("data: ") and not (prev or "").startswith("event:"):
                events.append(json.loads(line[6:]))
                if limit and len(events) >= limit:
                    break
            prev = line
        return events

    chat_id = authed_client_with_worker.post("/api/chats", json={"message": "Hello"}).json()["id"]
    assistant_id = authed_client_with_worker.get(f"/api/chats/{chat_id}/messages").json()[1]["id"]
    url = f"/api/chats/{chat_id}/messages/{assistant_id}/stream"

    first_raw: list[str] = []
    with authed_client_with_worker.stream("GET", url) as first_resp:
        assert first_resp.status_code == 200
        assert first_resp.headers["content-type"] == "text/event-stream; charset=utf-8"
        first_iter = first_resp.iter_lines()

        first_events = _collect_events(first_iter, first_raw, limit=3)

        with authed_client_with_worker.stream("GET", url) as second_resp:
            second_events = _collect_events(second_resp.iter_lines(), [])

        first_events += _collect_events(first_iter, first_raw)

    assert [e["seq_id"] for e in first_events] == list(range(len(first_events)))
    assert "".join(e["token"] for e in first_events) == TEST_RESPONSE
    assert "".join(e["token"] for e in second_events) == TEST_RESPONSE

    assert first_raw[-2] == "event: done"
    assert json.loads(first_raw[-1].removeprefix("data: ")) == {"status": "complete"}


@pytest.mark.parametrize(
    "settings",
    [{"event_stream_timeout": 0.05, "message_timeout": {"message_generation_timeout": 0.2}}],
    indirect=True,
)
def test_stream_emits_error_event_when_generation_times_out(authed_client):
    chat_id = authed_client.post("/api/chats", json={"message": "Hello"}).json()["id"]
    assistant_id = authed_client.get(f"/api/chats/{chat_id}/messages").json()[1]["id"]
    url = f"/api/chats/{chat_id}/messages/{assistant_id}/stream"

    with authed_client.stream("GET", url) as response:
        assert response.status_code == 200
        lines = list(response.iter_lines())

    assert "event: error" in lines
    data_line = lines[lines.index("event: error") + 1]
    assert json.loads(data_line.removeprefix("data: ")) == {"error": "message_generation_timeout"}


@pytest.mark.parametrize(
    "settings",
    [{"event_stream_timeout": 0.05, "message_timeout": {"message_generation_timeout": 0.3}}],
    indirect=True,
)
def test_reopening_stream_after_generation_timeout_receives_error(authed_client_with_worker, settings):
    chat_id = authed_client_with_worker.post("/api/chats", json={"message": "Hello"}).json()["id"]
    assistant_message = authed_client_with_worker.get(f"/api/chats/{chat_id}/messages").json()[1]
    assistant_id = assistant_message["id"]
    url = f"/api/chats/{chat_id}/messages/{assistant_id}/stream"

    with authed_client_with_worker.stream("GET", url) as first_resp:
        first_line = next(line for line in first_resp.iter_lines() if line.startswith("data: "))
    assert json.loads(first_line.removeprefix("data: "))["token"]

    sleep_until_message_timed_out(assistant_message, settings.message_timeout)

    with authed_client_with_worker.stream("GET", url) as second_resp:
        lines = list(second_resp.iter_lines())

    assert "event: error" in lines
    lines_before_error = lines[:lines.index("event: error")]
    assert not any(line.startswith("data: ") for line in lines_before_error)
    data_line = lines[lines.index("event: error") + 1]
    assert json.loads(data_line.removeprefix("data: ")) == {"error": "message_generation_timeout"}


def test_message_generated_via_llm_worker_completes_end_to_end(authed_client, llm_worker) -> None:
    chat_id = authed_client.post("/api/chats", json={"message": "Reply with exactly one word: 'PONG'"}).json()["id"]
    assistant_id = authed_client.get(f"/api/chats/{chat_id}/messages").json()[1]["id"]

    with authed_client.stream("GET", f"/api/chats/{chat_id}/messages/{assistant_id}/stream") as resp:
        list(resp.iter_lines())

    messages = authed_client.get(f"/api/chats/{chat_id}/messages").json()
    assert messages[1]["status"] == "complete"
    assert "pong" in messages[1]["content"].lower()
