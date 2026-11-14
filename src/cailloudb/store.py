import fcntl
import io
from abc import ABC, abstractmethod
from pathlib import Path
from typing import TYPE_CHECKING, AsyncIterator

from lsm.lsmtree import LSMTree
from settings import Settings

if TYPE_CHECKING:
    from lsm.engine_snapshot import EngineSnapshot
    from write_batch import WriteBatch


class _BaseStore(ABC):
    @abstractmethod
    async def get(
        self, key: bytes, engine_snapshot: EngineSnapshot | None
    ) -> bytes | None: ...

    @abstractmethod
    async def put(self, key: bytes, val: bytes) -> None: ...

    @abstractmethod
    async def delete(self, key: bytes) -> None: ...

    @abstractmethod
    async def write(self, batch: WriteBatch) -> None: ...

    @abstractmethod
    def scan(
        self,
        start: bytes | None,
        end: bytes | None,
        engine_snapshot: EngineSnapshot | None,
    ) -> AsyncIterator[tuple[bytes, bytes]]: ...

    @abstractmethod
    async def get_latest_sequence_number(self) -> int: ...


class BaseStore(_BaseStore):
    #: Runtime settings
    _settings: Settings

    #: Path & associated lock
    _path: Path
    _lock_file: io.TextIOWrapper | None

    #: LSMTree-based storage
    _tree: LSMTree

    def init(self) -> None:
        self._path.mkdir(parents=True, exist_ok=True)
        self._acquire_file_lock()

    def _acquire_file_lock(self) -> None:
        """
        Create a `LOCK` file in the store directory.
        """
        lock_file_path = self._path / "LOCK"
        self._lock_file = open(lock_file_path, "a+")

        try:
            fcntl.flock(
                self._lock_file.fileno(),
                fcntl.LOCK_EX  # Only 1 writer
                | fcntl.LOCK_NB,  # Explicit raise to exit other process
            )
        except IOError, OSError:
            self._lock_file.close()
            self._lock_file = None
            raise RuntimeError(f"Database lock failed for `{self._path}`.")

    def close(self) -> None:
        """
        Close the store gracefully and release the lock file.
        """
        if self._lock_file is not None:
            try:
                fcntl.flock(self._lock_file.fileno(), fcntl.LOCK_UN)
                self._lock_file.close()
            except Exception:
                pass
            finally:
                self._lock_file = None

    async def get(
        self, key: bytes, engine_snapshot: EngineSnapshot | None = None
    ) -> bytes | None:
        """
        Get a key.

        If the key is not found, return None.
        """
        return self._tree.get(key, engine_snapshot)

    async def put(self, key: bytes, value: bytes) -> None:
        """
        Put a key/value pair.
        """
        if not isinstance(value, bytes):
            raise ValueError("Type {} invalid for value.".format(type(value)))
        if not isinstance(key, bytes):
            raise KeyError("Type {} invalid for key.".format(type(key)))

        await self._tree.put(key, value)

    async def delete(self, key: bytes) -> None:
        """
        Delete a key.

        If the key doesn't exist, a deletion marker is created nonetheless.
        """

        await self._tree.delete(key)

    async def write(self, batch: WriteBatch) -> None:
        """
        Write a batch of operations in a single atomic operation.
        """
        # Assumptions:
        # - Single writer make this write essentially a blocking operation.
        # - Failure to apply this method should be retried during system recovery (crash).
        # - MVCC safeguards readers consistency during write runtime.
        await self._tree.write(batch)

    async def scan(
        self,
        start: bytes | None = None,
        end: bytes | None = None,
        engine_snapshot: EngineSnapshot | None = None,
    ) -> AsyncIterator[tuple[bytes, bytes]]:
        """
        Scan values between inclusive start and exclusive end.
        """
        for key, value in self._tree.scan(start, end, engine_snapshot):
            yield key, value

    def create_snapshot(self) -> EngineSnapshot:
        return self._tree.create_snapshot()

    def get_latest_sequence_number(self):
        return self._tree.get_latest_sequence_number()


class InMemoryStore(BaseStore):
    def __init__(self, path: Path | str, settings: Settings | None = None):
        self._settings = settings or Settings()
        self._path = Path(path)

        self.init()

        self._tree = LSMTree(
            settings=self._settings, durable_sstable=False, path=self._path
        )


class DurableStore(BaseStore):
    def __init__(self, path: Path | str, settings: Settings | None = None):
        self._settings = settings or Settings()
        self._path = Path(path)

        self.init()

        self._tree = LSMTree(
            settings=self._settings, durable_sstable=True, path=self._path
        )
