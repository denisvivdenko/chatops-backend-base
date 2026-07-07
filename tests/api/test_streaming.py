import json
import time
import pytest

from chatops.workers.worker import TEST_RESPONSE


def test_stream_assistant_response(authed_client_with_worker):

    def _collect_events(lines, limit=None):
        events = []
        for line in lines:
            if line.startswith("data: "):
                events.append(json.loads(line[6:]))
                if limit and len(events) >= limit:
                    break
        return events

    chat_id = authed_client_with_worker.post("/api/chats", json={"message": "Hello"}).json()["id"]
    assistant_id = authed_client_with_worker.get(f"/api/chats/{chat_id}/messages").json()[1]["id"]
    url = f"/api/chats/{chat_id}/messages/{assistant_id}/stream"

    with authed_client_with_worker.stream("GET", url) as first_resp:
        assert first_resp.status_code == 200
        assert first_resp.headers["content-type"] == "text/event-stream; charset=utf-8"
        first_iter = first_resp.iter_lines()

        first_events = _collect_events(first_iter, limit=3)

        with authed_client_with_worker.stream("GET", url) as second_resp:
            second_events = _collect_events(second_resp.iter_lines())

        first_events += _collect_events(first_iter)

    assert [e["seq_id"] for e in first_events] == list(range(len(first_events)))
    assert "".join(e["token"] for e in first_events) == TEST_RESPONSE
    assert "".join(e["token"] for e in second_events) == TEST_RESPONSE


@pytest.mark.parametrize(
    "settings",
    [{"event_stream_timeout": 0.05, "message_generation_timeout": 0.2}],
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
    [{"event_stream_timeout": 0.05, "message_generation_timeout": 0.3}],
    indirect=True,
)
def test_reopening_stream_after_generation_timeout_receives_error(authed_client_with_worker):
    chat_id = authed_client_with_worker.post("/api/chats", json={"message": "Hello"}).json()["id"]
    assistant_id = authed_client_with_worker.get(f"/api/chats/{chat_id}/messages").json()[1]["id"]
    url = f"/api/chats/{chat_id}/messages/{assistant_id}/stream"

    with authed_client_with_worker.stream("GET", url) as first_resp:
        first_line = next(line for line in first_resp.iter_lines() if line.startswith("data: "))
    assert json.loads(first_line.removeprefix("data: "))["token"]

    time.sleep(0.4)

    with authed_client_with_worker.stream("GET", url) as second_resp:
        lines = list(second_resp.iter_lines())

    assert "event: error" in lines
    lines_before_error = lines[:lines.index("event: error")]
    assert not any(line.startswith("data: ") for line in lines_before_error)
    data_line = lines[lines.index("event: error") + 1]
    assert json.loads(data_line.removeprefix("data: ")) == {"error": "message_generation_timeout"}
