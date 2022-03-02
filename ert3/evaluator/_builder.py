import pathlib
import shlex
from functools import partial
from typing import Callable, Dict, Tuple, Type, cast

import cloudpickle
from pydantic import FilePath

import ert
import ert3
from ert_shared.async_utils import get_event_loop
from ert_shared.ensemble_evaluator.ensemble.base import Ensemble
from ert_shared.ensemble_evaluator.ensemble.builder import (
    StepBuilder,
    create_ensemble_builder,
    create_input_builder,
    create_job_builder,
    create_output_builder,
    create_realization_builder,
)


def add_step_inputs(
    inputs: Tuple[ert3.config.LinkedInput, ...],
    transmitters: Dict[int, Dict[str, ert.data.RecordTransmitter]],
    step: StepBuilder,
) -> None:
    for input_ in inputs:
        step_input = (
            create_input_builder()
            .set_name(input_.name)
            .set_transformation(input_.transformation)
        )

        for iens, io_to_transmitter in transmitters.items():
            trans = io_to_transmitter[input_.name]
            # cast necessary due to https://github.com/python/mypy/issues/9656
            step_input.set_transmitter_factory(
                lambda _t=trans: cast(ert.data.RecordTransmitter, _t), iens
            )
        step.add_input(step_input)


def add_commands(
    transportable_commands: Tuple[ert3.config.TransportableCommand, ...],
    storage_type: str,
    storage_path: str,
    step: StepBuilder,
) -> None:
    def command_location(name: str) -> FilePath:
        return next(
            (cmd.location for cmd in transportable_commands if cmd.name == name),
            pathlib.Path(name),
        )

    async def transform_output(
        name: str,
        transmitter: ert.data.RecordTransmitter,
        location: pathlib.Path,
    ) -> None:
        transformation = ert.data.ExecutableTransformation(location)
        record = await transformation.to_record()
        await transmitter.transmit_record(record)
        step.add_input(
            create_input_builder()
            .set_name(name)
            .set_transformation(transformation)
            # cast necessary due to https://github.com/python/mypy/issues/9656
            .set_transmitter_factory(
                lambda _t=transmitter: cast(ert.data.RecordTransmitter, _t)
            )
        )

    for command in transportable_commands:
        transmitter: ert.data.RecordTransmitter
        if storage_type == "shared_disk":
            transmitter = ert.data.SharedDiskRecordTransmitter(
                name=command.name,
                storage_path=pathlib.Path(storage_path),
            )
        elif storage_type == "ert_storage":
            transmitter = ert.storage.StorageRecordTransmitter(
                name=command.name, storage_url=storage_path
            )
        else:
            raise ValueError(f"Unsupported transmitter type: {storage_type}")
        get_event_loop().run_until_complete(
            transform_output(
                name=command.name,
                transmitter=transmitter,
                location=command.location,
            )
        )


def add_step_outputs(
    storage_type: str,
    step_config: ert3.config.Step,
    storage_path: str,
    ensemble_size: int,
    step: StepBuilder,
) -> None:
    for record_name, output in step_config.output.items():
        transformation = output.get_transformation_instance()
        output = (
            create_output_builder()
            .set_name(record_name)
            .set_transformation(transformation)
        )
        for iens in range(0, ensemble_size):
            factory: Callable[
                [Type[ert.data.RecordTransmitter]], ert.data.RecordTransmitter
            ]
            if storage_type == "shared_disk":
                factory = partial(
                    ert.data.SharedDiskRecordTransmitter,
                    name=record_name,
                    storage_path=pathlib.Path(storage_path),
                )
            elif storage_type == "ert_storage":
                factory = partial(
                    ert.storage.StorageRecordTransmitter,
                    name=record_name,
                    storage_url=storage_path,
                    iens=iens,
                )
            else:
                raise ValueError(
                    f"unexpected storage type{storage_type} for {record_name} record"
                )
            output.set_transmitter_factory(factory, iens)
        step.add_output(output)


def build_ensemble(
    stage: ert3.config.Step,
    driver: str,
    ensemble_size: int,
    step_builder: StepBuilder,
) -> Ensemble:
    if isinstance(stage, ert3.config.Function):
        step_builder.add_job(
            create_job_builder()
            .set_name(stage.function.__name__)
            .set_executable(cloudpickle.dumps(stage.function))
        )
    if isinstance(stage, ert3.config.Unix):

        def command_location(name: str) -> FilePath:
            assert isinstance(stage, ert3.config.Unix)  # mypy
            return next(
                (
                    cmd.location
                    for cmd in stage.transportable_commands
                    if cmd.name == name
                ),
                pathlib.Path(name),
            )

        for script in stage.script:
            name, *args = shlex.split(script)
            step_builder.add_job(
                create_job_builder()
                .set_name(name)
                .set_executable(command_location(name))
                .set_args(tuple(args))
            )

    builder = (
        create_ensemble_builder()
        .set_ensemble_size(ensemble_size)
        .set_max_running(10000)
        .set_max_retries(0)
        .set_executor(driver)
        .set_forward_model(
            create_realization_builder().active(True).add_step(step_builder)
        )
    )

    return builder.build()
