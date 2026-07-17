from pathlib import Path


class ResourceStorage:
    def __init__(self, base_dir: Path) -> None:
        self._base_dir = Path(base_dir)
        self._base_dir.mkdir(parents=True, exist_ok=True)

    def save(self, resource_id: str, content: bytes) -> str:
        file_path = self._base_dir / resource_id
        file_path.write_bytes(content)
        return str(file_path)

    def read(self, file_path: str) -> bytes:
        return Path(file_path).read_bytes()

    def delete(self, file_path: str) -> None:
        Path(file_path).unlink(missing_ok=True)
