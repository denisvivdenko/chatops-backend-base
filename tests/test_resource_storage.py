from pathlib import Path

from chatops.storage.resource_storage import ResourceStorage


def test_save_writes_file_at_returned_path(tmp_path: Path) -> None:
    storage = ResourceStorage(tmp_path)

    file_path = storage.save("resource-1", b"%PDF-1.4 fake content")

    assert Path(file_path).exists()
    assert Path(file_path).read_bytes() == b"%PDF-1.4 fake content"


def test_read_round_trips_saved_content(tmp_path: Path) -> None:
    storage = ResourceStorage(tmp_path)
    file_path = storage.save("resource-1", b"some bytes")

    assert storage.read(file_path) == b"some bytes"
