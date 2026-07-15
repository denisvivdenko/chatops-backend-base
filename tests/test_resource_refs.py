import pytest

from chatops.services.resource_refs import parse_resource_refs


def test_no_refs_returns_empty_list() -> None:
    assert parse_resource_refs("just some plain text") == []


def test_single_ref_returns_artifact_id() -> None:
    assert parse_resource_refs("[report.pdf](resource://abc123)") == ["abc123"]


def test_multiple_refs_returned_in_order_with_duplicates_preserved() -> None:
    content = "[a.pdf](resource://id1) [b.pdf](resource://id2) [a-again.pdf](resource://id1)"
    assert parse_resource_refs(content) == ["id1", "id2", "id1"]


def test_ref_embedded_alongside_plain_text() -> None:
    content = "Please review [spec.pdf](resource://id1) before the meeting."
    assert parse_resource_refs(content) == ["id1"]


@pytest.mark.parametrize(
    "content",
    [
        "[report.pdf](http://example.com/id1)",
        "[report.pdf](resource://id1",
        "resource://id1",
        "[report.pdf]resource://id1)",
    ],
)
def test_malformed_or_near_miss_patterns_are_ignored(content: str) -> None:
    assert parse_resource_refs(content) == []
