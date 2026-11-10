from typing import TypedDict


class SerializedFileMetadata(TypedDict):
    file_id: int
    smallest_key: str
    largest_key: str
    file_size: int


class FileMetadata(TypedDict):
    """
    Represents an SSTable file on disk.
    """

    file_id: int
    smallest_key: bytes
    largest_key: bytes
    file_size: int
