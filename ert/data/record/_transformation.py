from enum import Enum, Flag, auto
import io
import stat
import tarfile
from abc import ABC, abstractmethod
from concurrent import futures
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Union

import aiofiles
from ecl.summary import EclSum

from ert.data import (
    BlobRecord,
    NumericalRecord,
    NumericalRecordTree,
    Record,
)
from ert.serialization import get_serializer, has_serializer
from ert_shared.async_utils import get_event_loop

_BIN_FOLDER = "bin"


class RecordTransformationDirectionality(Flag):
    """Defines how transformations map from files to records, and/or the inverse.
    NONE: Cannot transform records.
    FROM_RECORD: Can transform from record to e.g. a file.
    TO_RECORD: Can transform to record a record from e.g. a file.
    BIDIRECTIONAL: Can transform both ways.
    """

    NONE = auto()
    FROM_RECORD = auto()
    TO_RECORD = auto()
    BIDIRECTIONAL = FROM_RECORD | TO_RECORD

    @classmethod
    def from_direction(cls, s: str) -> "RecordTransformationDirectionality":
        if s == "input":
            return cls.FROM_RECORD
        elif s == "output":
            return cls.TO_RECORD
        elif s == "both":
            return cls.BIDIRECTIONAL
        elif s == "none":
            return cls.NONE
        raise ValueError(f"unknown RecordTransformationDirectionality for '{s}'")

    def __str__(self) -> str:
        if self == self.__class__.FROM_RECORD:
            return "input"
        elif self == self.__class__.TO_RECORD:
            return "output"
        elif self == self.__class__.BIDIRECTIONAL:
            return "both"
        elif self == self.__class__.NONE:
            return "none"
        raise ValueError


def _prepare_location(root_path: Path, location: Path) -> None:
    """Ensure relative, and if the location is within a folder create those
    folders."""
    if location.is_absolute():
        raise ValueError(f"location {location} must be relative")
    abs_path = root_path / location
    if not abs_path.parent.exists():
        abs_path.parent.mkdir(parents=True, exist_ok=True)


def _sync_make_tar(file_path: Path) -> bytes:
    tar_obj = io.BytesIO()
    with tarfile.open(fileobj=tar_obj, mode="w") as tar:
        tar.add(file_path, arcname="")
    return tar_obj.getvalue()


async def _make_tar(file_path: Path) -> bytes:
    """Walk the files under a filepath and encapsulate the file(s)
    in a tar package.

    Args:
        file_path: The path to convert to tar.

    Returns:
        bytes: bytes from in memory tar object.
    """
    executor = futures.ThreadPoolExecutor()
    return await get_event_loop().run_in_executor(executor, _sync_make_tar, file_path)


class RecordTransformationAffinity(Flag):
    OPAQUE = auto()
    NON_OPAQUE = auto()
    NONE = OPAQUE | NON_OPAQUE


class RecordTransformation(ABC):
    """:class:`RecordTransformation` is an abstract class that handles
    custom save and load operations on Records to and from disk.

    The direction parameter can be used to point the transformation in
    a specific direction. This direction will then be compared to the
    transformation's DIRECTIONALITY, which constraints the transformation
    to a specific direction, no direction or both (from/to). This aids
    validation where direction is known, and actual transformation is
    deferred.
    """

    DIRECTIONALITY = RecordTransformationDirectionality.NONE

    def __init__(
        self,
        direction: Optional[RecordTransformationDirectionality] = None,
    ) -> None:
        super().__init__()
        if not direction:
            direction = self.DIRECTIONALITY
        if direction not in self.DIRECTIONALITY:
            raise ValueError(f"{self} cannot transform in direction: {str(direction)}")
        self.direction = direction

    @abstractmethod
    async def from_record(self, record: Record, root_path: Path = Path()) -> None:
        pass

    @abstractmethod
    async def to_record(self, root_path: Path = Path()) -> Record:
        pass


class FileTransformation(RecordTransformation):

    MIME = "application/octet-stream"

    def __init__(
        self,
        location: Path,
        mime: Optional[str] = None,
        direction: Optional[RecordTransformationDirectionality] = None,
    ) -> None:
        super().__init__(direction)
        if not mime:
            mime = self.MIME
        self.mime = mime
        self.location = location
        self.affinity = self._infer_record_affinity()

    async def from_record(self, record: Record, root_path: Path = Path()) -> None:
        raise NotImplementedError("not implemented")

    async def to_record(self, root_path: Path = Path()) -> Record:
        raise NotImplementedError("not implemented")

    def _infer_record_affinity(self) -> RecordTransformationAffinity:
        if has_serializer(self.mime) or self.mime == EclSumTransformation.MIME:
            return RecordTransformationAffinity.NON_OPAQUE
        return RecordTransformationAffinity.OPAQUE


class SerializationTransformation(FileTransformation):
    """:class:`SerializationTransformation` is :class:`RecordTransformation`
    implementation which provides basic Record to disk and disk to Record
    functionality.
    """

    DIRECTIONALITY = RecordTransformationDirectionality.BIDIRECTIONAL

    async def from_record(self, record: Record, root_path: Path = Path()) -> None:
        """Transforms a Record to disk on the given location via a serializer
        given in the mime type.

        Args:
            record: a record object to save to disk
            root_path: the root of the path
        """
        if not isinstance(record, (NumericalRecord, BlobRecord)):
            raise TypeError("Record type must be a NumericalRecord or BlobRecord")

        _prepare_location(root_path, self.location)
        await _save_record_to_file(record, root_path / self.location, self.mime)

    async def to_record(self, root_path: Path = Path()) -> Record:
        """Transfroms a file to Record from the given location via
        a serializer given in the mime type.

        Args:
            root_path: the root of the path

        Returns:
            Record: the object is either :class:`BlobRecord` or
                :class:`NumericalRecord`
        """
        return await _load_record_from_file(root_path / self.location, self.mime)


class TarTransformation(FileTransformation):
    """:class:`TarTransformation` is :class:`RecordTransformation`
    implementation which provides creating a tar object from a given location
    into a BlobRecord :func:`TarTransformation.to_record` and
    extracting tar object (:class:`BlobRecord`) to the given location.
    """

    MIME = "application/x-directory"

    DIRECTIONALITY = RecordTransformationDirectionality.BIDIRECTIONAL

    async def from_record(self, record: Record, root_path: Path = Path()) -> None:
        """Transforms BlobRecord (tar object) to disk, ie. extracting tar object
        on the given location.

        Args:
            record: BlobRecord object, where :func:`BlobRecord.data`
                is the binary tar object
            root_path: the root of the path

        Raises:
            TypeError: Raises when the Record (loaded via transmitter)
                is not BlobRecord
        """
        if not isinstance(record, BlobRecord):
            raise TypeError("Record type must be a BlobRecord")

        with tarfile.open(fileobj=io.BytesIO(record.data), mode="r") as tar:
            _prepare_location(root_path, self.location)
            tar.extractall(root_path / self.location)

    async def to_record(self, root_path: Path = Path()) -> Record:
        """Transfroms directory from the given location into a :class:`BlobRecord`
        object.

        Args:
            root_path: the root of the path

        Returns:
            Record: returns :class:`BlobRecord` object that is a
                binary representation of the final tar object.
        """
        return BlobRecord(data=await _make_tar(root_path / self.location))


class ExecutableTransformation(SerializationTransformation):
    """:class:`ExecutableTransformation` is :class:`RecordTransformation`
    implementation which provides creating an executable file; ie. when
    storing a Record to the file.
    """

    DIRECTIONALITY = RecordTransformationDirectionality.BIDIRECTIONAL

    async def from_record(self, record: Record, root_path: Path = Path()) -> None:
        """Transforms a Record to disk on the given location via
        via a serializer given in the mime type. Additionally, it makes
        executable from the file

        Args:
            record: a record object to save to disk that becomes executable
            root_path: the root of the path
        """
        if not isinstance(record, BlobRecord):
            raise TypeError("Record type must be a BlobRecord")

        # pre-make bin folder if necessary
        root_path = Path(root_path / _BIN_FOLDER)
        root_path.mkdir(parents=True, exist_ok=True)

        # create file(s)
        _prepare_location(root_path, self.location)
        await _save_record_to_file(record, root_path / self.location, self.mime)

        # post-process if necessary
        path = root_path / self.location
        st = path.stat()
        path.chmod(st.st_mode | stat.S_IEXEC)

    async def to_record(self, root_path: Path = Path()) -> Record:
        """Transforms a file to Record from the given location via
        a serializer given in the mime type..

        Args:
            root_path: the root of the path (default: the empty Path)

        Returns:
            Record: return object of :class:`BlobRecord` type.
        """
        return await _load_record_from_file(
            root_path / self.location, "application/octet-stream"
        )


async def _load_record_from_file(file: Path, mime: str) -> Record:
    if mime == "application/octet-stream":
        async with aiofiles.open(str(file), mode="rb") as fb:
            contents_b: bytes = await fb.read()
            return BlobRecord(data=contents_b)
    else:
        serializer = get_serializer(mime)
        _record_data = await serializer.decode_from_path(file)
        return NumericalRecord(data=_record_data)


async def _save_record_to_file(record: Record, location: Path, mime: str) -> None:
    if isinstance(record, NumericalRecord):
        async with aiofiles.open(str(location), mode="wt", encoding="utf-8") as ft:
            await ft.write(get_serializer(mime).encode(record.data))
    else:
        async with aiofiles.open(str(location), mode="wb") as fb:
            assert isinstance(record.data, bytes)  # mypy
            await fb.write(record.data)


class RecordTreeTransformation(SerializationTransformation):
    """
    Write all leaf records of a NumericalRecordTree to individual files.
    """

    DIRECTIONALITY = RecordTransformationDirectionality.FROM_RECORD

    def __init__(
        self,
        location: Path,
        mime: Optional[str] = None,
        sub_path: Optional[str] = None,
        direction: Optional[RecordTransformationDirectionality] = None,
    ):
        if sub_path is not None:
            raise NotImplementedError("Extracting sub-trees not implemented")
        super().__init__(location, mime, direction)

    async def from_record(self, record: Record, root_path: Path = Path()) -> None:
        if not isinstance(record, NumericalRecordTree):
            raise TypeError("Only NumericalRecordTrees can be transformed.")
        for key, leaf_record in record.flat_record_dict.items():
            location_key = f"{key}-{self.location}"
            await get_serializer(self.mime).encode_to_path(
                leaf_record.data, path=root_path / location_key
            )

    async def to_record(self, root_path: Path = Path()) -> Record:
        raise NotImplementedError


class EclSumTransformation(FileTransformation):
    """Transform binary output from Eclipse into a NumericalRecordTree."""

    MIME = "application/x-eclipse.summary"

    DIRECTIONALITY = RecordTransformationDirectionality.TO_RECORD

    def __init__(
        self,
        location: Path,
        smry_keys: List[str],
        direction: RecordTransformationDirectionality = RecordTransformationDirectionality.TO_RECORD,
    ):
        """
        Args:
            location: Path location of eclipse load case, passed as load_case to EclSum.
            smry_keys: List (non-empty) of Eclipse summary vectors (must be present) to
                include when transforming from Eclipse binary files. Wildcards are not
                supported.
        """
        super().__init__(location, self.MIME, direction)
        if not smry_keys:
            raise ValueError("smry_keys must be non-empty")
        self._smry_keys = smry_keys

    async def to_record(self, root_path: Path = Path()) -> Record:
        executor = futures.ThreadPoolExecutor()
        record_dict = await get_event_loop().run_in_executor(
            executor,
            _sync_eclsum_to_record,
            root_path / self.location,
            self._smry_keys,
        )
        return NumericalRecordTree(record_dict=record_dict)

    @property
    def smry_keys(self) -> List[str]:
        return self._smry_keys


def _sync_eclsum_to_record(
    location: Path, smry_keys: List[str]
) -> Dict[str, NumericalRecord]:
    eclsum = EclSum(str(location))
    record_dict = {}
    for key in smry_keys:
        record_dict[key] = NumericalRecord(
            data=dict(zip(map(str, eclsum.dates), map(float, eclsum.numpy_vector(key))))
        )
    return record_dict
