import pytest

from chatops.services.resource_service import ResourceService


def test_parse_resource_refs_multiple_refs_returned_in_order_with_duplicates_preserved() -> None:
    content = "[a.pdf](resource://id1) [b.pdf](resource://id2) [a-again.pdf](resource://id1)"
    assert ResourceService.parse_resource_refs(content) == ["id1", "id2", "id1"]


@pytest.mark.parametrize(
    "content",
    [
        "[report.pdf](http://example.com/id1)",
        "[report.pdf](resource://id1",
        "resource://id1",
        "[report.pdf]resource://id1)",
    ],
)
def test_parse_resource_refs_malformed_or_near_miss_patterns_are_ignored(content: str) -> None:
    assert ResourceService.parse_resource_refs(content) == []
