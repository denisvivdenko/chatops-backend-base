import re

RESOURCE_REF_PATTERN = re.compile(r"\[[^\]]+\]\(resource://([^)]+)\)")


def parse_resource_refs(content: str) -> list[str]:
    return RESOURCE_REF_PATTERN.findall(content)
