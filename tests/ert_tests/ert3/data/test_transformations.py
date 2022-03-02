import contextlib
import datetime
from functools import partial
import io
import json
import os
import pathlib
import re
import tarfile
from typing import Callable, ContextManager, List, Type

import numpy as np
import pytest
from ecl.summary import EclSum
from ert_utils import tmp

from ert.data import (
    Record,
    BlobRecord,
    EclSumTransformation,
    ExecutableTransformation,
    SerializationTransformation,
    NumericalRecord,
    NumericalRecordTree,
    RecordTransformation,
    RecordTransformationDirectionality,
    RecordTransmitter,
    RecordTreeTransformation,
    TarTransformation,
)


@contextlib.contextmanager
def file_factory_context(tmpdir):
    def file_factory(files: List[str]) -> None:
        for file in files:
            if file == "TEST.UNSMRY":
                _create_synthetic_smry(tmpdir)
            if file == "TEST.SMSPEC":
                pass
            dir_path = pathlib.Path(tmpdir) / file
            dir_path.parent.mkdir(parents=True, exist_ok=True)
            dir_path.touch()

    yield file_factory


@contextlib.contextmanager
def record_factory_context(tmpdir):
    def record_factory(type: str):
        if type == "dir":
            dir_path = pathlib.Path(tmpdir) / "resources" / "test_dir"
            _files = [dir_path / "a.txt", dir_path / "b.txt"]
            with file_factory_context(tmpdir) as file_factory:
                file_factory(_files)
            tar_obj = io.BytesIO()
            with tarfile.open(fileobj=tar_obj, mode="w") as tar:
                tar.add(dir_path, arcname="")
            tardata = tar_obj.getvalue()
            return BlobRecord(data=tardata)
        elif type == "tree":
            return NumericalRecordTree(
                record_dict={
                    "a": NumericalRecord(data=[1, 2]),
                    "b": NumericalRecord(data=[3, 4]),
                }
            )
        elif type == "blob":
            return BlobRecord(data=b"\xF0\x9F\xA6\x89")
        elif type == "eclsum":
            return None
        else:
            raise ValueError(f"type {type} not recognized")

    yield record_factory


from_record_params = pytest.mark.parametrize(
    ("cls,args,type,files"),
    (
        pytest.param(
            SerializationTransformation,
            [pathlib.Path("test.blob"), "application/octet-stream"],
            "blob",
            ["test.blob"],
        ),
        pytest.param(
            TarTransformation,
            [pathlib.Path("test_dir")],
            "dir",
            ["test_dir/a.txt", "test_dir/b.txt"],
        ),
        pytest.param(
            ExecutableTransformation,
            [pathlib.Path("test.blob"), "application/octet-stream"],
            "blob",
            ["bin/test.blob"],
        ),
        pytest.param(
            RecordTreeTransformation,
            [pathlib.Path("leaf.json"), "application/json"],
            "tree",
            ["a-leaf.json", "b-leaf.json"],
        ),
        pytest.param(
            EclSumTransformation,
            ["TEST.UNSMRY", ["FOPT", "FOPR"]],
            "eclsum",
            None,
            marks=pytest.mark.raises(
                exception=ValueError,
                match=r".+cannot transform in direction: input",
                match_flags=(re.MULTILINE | re.DOTALL),
            ),
        ),
    ),
)
to_record_params = pytest.mark.parametrize(
    ("cls,args,files,record_cls"),
    (
        pytest.param(
            SerializationTransformation,
            [pathlib.Path("test.blob"), "application/octet-stream"],
            ["test.blob"],
            BlobRecord,
        ),
        pytest.param(
            TarTransformation,
            [pathlib.Path("test_dir")],
            ["test_dir/a.txt", "test_dir/b.txt"],
            BlobRecord,
        ),
        pytest.param(
            ExecutableTransformation,
            [pathlib.Path("bin/test.blob"), "application/octet-stream"],
            ["bin/test.blob"],
            BlobRecord,
        ),
        pytest.param(
            RecordTreeTransformation,
            [pathlib.Path("a-leaf.json"), "application/json"],
            ["a-leaf.json", "b-leaf.json"],
            None,
            marks=pytest.mark.skip("bare RecordTreeTransformation isn't testable"),
        ),
        pytest.param(
            EclSumTransformation,
            ["TEST.UNSMRY", ["FOPT", "FOPR"]],
            ["TEST.UNSMRY", "TEST.SMSPEC"],
            NumericalRecordTree,
        ),
    ),
)


def _create_synthetic_smry(directory_path: pathlib.Path, length: int = 3):
    """Create synthetic TEST.UNSMRY and TEST.SMSPEC files in a specified directory"""
    sum_keys = {
        "FOPT": [i for i in range(length)],
        "FOPR": [1] * length,
    }
    dimensions = [10, 10, 10]
    ecl_sum = EclSum.writer("TEST", datetime.date(2000, 1, 1), *dimensions)

    for key in sum_keys:
        ecl_sum.add_variable(key)

    for val, idx in enumerate(range(0, length, 1)):
        t_step = ecl_sum.add_t_step(idx, val)
        for key, item in sum_keys.items():
            t_step[key] = item[idx]

    # libecl can only write UNSMRY+SMSPEC files to current working directory
    old_dir = os.getcwd()
    try:
        os.chdir(directory_path)
        ecl_sum.fwrite()
    finally:
        os.chdir(old_dir)


@pytest.mark.asyncio
@from_record_params
async def test_atomic_from_record_transformation(
    record_transmitter_factory_context: ContextManager[
        Callable[[str], RecordTransmitter]
    ],
    cls: Type[RecordTransformation],
    args: list,
    type: str,
    files: List[str],
    storage_path,
    tmp_path,
):
    runpath = pathlib.Path(".")
    with record_transmitter_factory_context(
        storage_path=storage_path
    ) as record_transmitter_factory, record_factory_context(
        tmp_path
    ) as record_factory, tmp():
        # TODO: https://github.com/python/mypy/issues/6799
        transformation = cls(*args, direction=RecordTransformationDirectionality.FROM_RECORD)  # type: ignore

        record_in = record_factory(type=type)
        transmitter = record_transmitter_factory(name="trans_custom")
        await transmitter.transmit_record(record_in)
        assert transmitter.is_transmitted()
        record = await transmitter.load()
        await transformation.from_record(record)
        for file in files:
            assert (runpath / file).exists()


@pytest.mark.asyncio
@to_record_params
async def test_atomic_to_record_transformation(
    record_transmitter_factory_context: ContextManager[
        Callable[[str], RecordTransmitter]
    ],
    cls: Type[RecordTransformation],
    args: list,
    files: List[str],
    record_cls: Type[Record],
    storage_path,
    tmp_path,
):
    with record_transmitter_factory_context(
        storage_path=storage_path
    ) as record_transmitter_factory, file_factory_context(
        tmp_path
    ) as file_factory, tmp():
        runpath = pathlib.Path(tmp_path)
        file_factory(files=files)
        transmitter = record_transmitter_factory(name="trans_custom")
        assert transmitter.is_transmitted() is False

        # TODO: https://github.com/python/mypy/issues/6799
        transformation = cls(*args, direction=RecordTransformationDirectionality.TO_RECORD)  # type: ignore

        record = await transformation.to_record(root_path=runpath)
        await transmitter.transmit_record(record)
        assert transmitter.is_transmitted()

        loaded_record = await transmitter.load()
        assert isinstance(loaded_record, record_cls)


# @pytest.mark.asyncio
# async def test_transform_output_sequence(tmpdir):
#     test_data = [
#         {"a": 1.0, "b": 2.0},
#         {"a": 3.0, "b": 4.0},
#         {"a": 5.0, "b": 6.0},
#     ]
#     file = pathlib.Path(tmpdir) / "test.json"
#     with open(file, "w", encoding="utf-8") as fp:
#         json.dump(test_data, fp)

#     records = await SerializationTransformation(
#         location=file, mime="application/json"
#     ).transform_output_sequence(file)
#     assert len(records) == 3
#     for record, data in zip(records, test_data):
#         assert isinstance(record, NumericalRecord)
#         assert data == record.data


@pytest.mark.asyncio
async def test_eclsum_transformation(tmp_path):
    _create_synthetic_smry(tmp_path, length=3)
    numrecordtree = await EclSumTransformation(
        location=tmp_path / "TEST",
        smry_keys=["FOPT"],
        direction=RecordTransformationDirectionality.TO_RECORD,
    ).to_record()
    data = numrecordtree.flat_record_dict["FOPT"].data
    assert data == {
        "2000-01-01 00:00:00": 0,
        "2000-01-02 00:00:00": 1,
        "2000-01-03 00:00:00": 2,
    }

    for _float in data.values():
        # Avoid numpy types
        assert not isinstance(_float, np.floating)


@pytest.mark.asyncio
async def test_eclsum_transformation_wrongkey(tmp_path):
    _create_synthetic_smry(tmp_path, length=3)
    with pytest.raises(KeyError):
        await EclSumTransformation(
            location=tmp_path / "TEST",
            smry_keys=["BOGUS"],
            direction=RecordTransformationDirectionality.TO_RECORD,
        ).to_record()


@pytest.mark.asyncio
async def test_eclsum_transformation_emptykeys(tmp_path):
    _create_synthetic_smry(tmp_path, length=3)
    with pytest.raises(ValueError):
        await EclSumTransformation(
            location=tmp_path / "TEST",
            smry_keys=[],
            direction=RecordTransformationDirectionality.TO_RECORD,
        ).to_record()
