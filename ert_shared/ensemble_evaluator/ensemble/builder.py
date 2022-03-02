import copy
import logging
import pickle
import uuid
from typing import (
    IO,
    TYPE_CHECKING,
    Callable,
    Dict,
    List,
    Tuple,
    Optional,
    Iterator,
    Type,
    TypeVar,
)
from collections import defaultdict
from graphlib import TopologicalSorter
from pathlib import Path
from enum import Enum

from ert_shared.ensemble_evaluator.ensemble.base import _Ensemble
from ert_shared.ensemble_evaluator.ensemble.legacy import _LegacyEnsemble
from ert_shared.ensemble_evaluator.ensemble.prefect import PrefectEnsemble
from ert_shared.ensemble_evaluator.ensemble.io_map import InputMap, OutputMap
from ert_shared.ensemble_evaluator.entity.function_step import FunctionTask
from ert_shared.ensemble_evaluator.entity.unix_step import UnixTask
from ert_shared.ensemble_evaluator.entity import identifiers as ids
from ert.data import RecordTransformation

from res.enkf import EnKFState, RunArg

if TYPE_CHECKING:
    import ert


logger = logging.getLogger(__name__)


_SOURCE_TEMPLATE_BASE = "/ert/ee/{ee_id}/"
_SOURCE_TEMPLATE_REAL = "/real/{iens}"
_SOURCE_TEMPLATE_STEP = "/step/{step_id}"
_SOURCE_TEMPLATE_JOB = "/job/{job_id}"


def _sort_steps(steps: List["_Step"]) -> Tuple[str, ...]:
    """Return a tuple comprised by step names in the order they should be
    executed."""
    graph = defaultdict(set)
    if len(steps) == 1:
        return (steps[0].get_name(),)
    edged_nodes = set()
    for step in steps:
        for other in steps:
            if step == other:
                continue
            step_outputs = set([io.get_name() for io in step.get_outputs()])
            other_inputs = set([io.get_name() for io in other.get_inputs()])
            if len(step_outputs) > 0 and not step_outputs.isdisjoint(other_inputs):
                graph[other.get_name()].add(step.get_name())
                edged_nodes.add(step.get_name())
                edged_nodes.add(other.get_name())

    isolated_nodes = set([step.get_name() for step in steps]) - edged_nodes
    for node in isolated_nodes:
        graph[node] = set()

    ts = TopologicalSorter(graph)
    return tuple(ts.static_order())


class _IO:
    def __init__(self, name, transformation: Optional[RecordTransformation] = None):
        self._transformation: RecordTransformation = transformation
        if not name:
            raise ValueError(f"{self} needs name")
        self._name = name

    def get_transformation(self):
        return self._transformation

    def get_name(self):
        return self._name

    def get_transmitter(self) -> None:
        raise NotImplementedError("IO should not have a transmitter")


_IOBuilder_TV = TypeVar("_IOBuilder_TV", bound="_IOBuilder")


class _IOBuilder:
    _concrete_cls: Optional[Type[_IO]] = None

    _TRANSMITTER_FACTORY_ALL = -1

    def __init__(self):
        self._name = None
        self._transformation: Optional[RecordTransformation] = None
        self._transmitter_factories: Dict[int, ert.data.transmitter_factory] = {}

    def set_name(self: _IOBuilder_TV, name) -> _IOBuilder_TV:
        self._name = name
        return self

    def set_transformation(self, transformation: RecordTransformation) -> "_IOBuilder":
        self._transformation = transformation
        return self

    def set_transmitter_factory(
        self, factory: "ert.data.transmitter_factory", index: Optional[int] = None
    ) -> "_IOBuilder":
        """Fix the transmitter factory for this IO to index if index is >= 0. If the
        index is omitted or is < 0, it the factory will be called for all indices not
        fixed to an index.
        """
        if index is None or index < 0:
            index = self._TRANSMITTER_FACTORY_ALL
        self._transmitter_factories[index] = factory
        return self

    def transmitter_factory(self, index: Optional[int] = None):
        """Return a fixed transmitter for index, a ensemble-wide transmitter if index is
        -1, or None.
        """
        global_factory = self._TRANSMITTER_FACTORY_ALL in self._transmitter_factories
        if index is None or index == self._TRANSMITTER_FACTORY_ALL:
            if not global_factory:
                return None
        if index not in self._transmitter_factories:
            if not global_factory:
                return None
            return self._transmitter_factories[self._TRANSMITTER_FACTORY_ALL]
        return self._transmitter_factories[index]

    def build(self):
        if self._concrete_cls is None:
            raise TypeError("cannot build _IO")
        return self._concrete_cls(self._name, transformation=self._transformation)


class _DummyIO(_IO):
    pass


class _DummyIOBuilder(_IOBuilder):
    _concrete_cls = _DummyIO

    def build(self):
        super().set_name("dummy i/o")
        return super().build()


# class _FileIO(_IO):
#     def __init__(
#         self,
#         name: str,
#         path: Path,
#         transformation: Optional[RecordTransformation] = None,
#     ) -> None:
#         super().__init__(name, transformation=transformation)
#         self._path = path

#     def get_path(self):
#         return self._path


# class _FileIOBuilder(_IOBuilder):
#     def __init__(self) -> None:
#         super().__init__()
#         self._path: Optional[Path] = None
#         self._cls = _FileIO

#     def set_path(self, path: Path) -> "_FileIOBuilder":
#         self._path = path
#         return self

#     def build(self):
#         return self._cls(self._name, self._path, self._transformation)


# def create_file_io_builder() -> _FileIOBuilder:
#     return _FileIOBuilder()


class _Input(_IO):
    pass


class _InputBuilder(_IOBuilder):
    _concrete_cls = _Input


def create_input_builder():
    return _InputBuilder()


class _Output(_IO):
    pass


class _OutputBuilder(_IOBuilder):
    _concrete_cls = _Output


def create_output_builder():
    return _OutputBuilder()


_BaseJobBuilder_TV = TypeVar("_BaseJobBuilder_TV", bound="_BaseJobBuilder")


class _BaseJobBuilder:
    def __init__(self):
        self._id = None
        self._name = None

    def reset(self: _BaseJobBuilder_TV) -> _BaseJobBuilder_TV:
        self._id = None
        self._name = None
        return self

    def set_id(self: _BaseJobBuilder_TV, id_) -> _BaseJobBuilder_TV:
        self._id = id_
        return self

    def set_parent_source(self: _BaseJobBuilder_TV, source) -> _BaseJobBuilder_TV:
        self._parent_source = source
        return self

    def set_name(self: _BaseJobBuilder_TV, name) -> _BaseJobBuilder_TV:
        self._name = name
        return self

    def build(_):
        raise NotImplementedError("cannot build basejob")


class _JobBuilder(_BaseJobBuilder):
    def __init__(self):
        super().__init__()
        self._executable = None
        self._args = None
        self._parent_source = None

    def set_executable(self, executable) -> "_JobBuilder":
        self._executable = executable
        return self

    def set_args(self, args) -> "_JobBuilder":
        self._args = args
        return self

    def build(self):
        if self._id is None:
            self._id = str(uuid.uuid4())
        source = (
            _SOURCE_TEMPLATE_BASE
            + self._parent_source
            + _SOURCE_TEMPLATE_JOB.format(job_id=self._id)
        )
        try:
            cmd_is_callable = callable(pickle.loads(self._executable))
        except TypeError:
            cmd_is_callable = False
        if cmd_is_callable:
            if self._args is not None:
                raise ValueError(
                    "callable executable does not take args, use inputs instead"
                )
            return _FunctionJob(self._id, self._name, source, self._executable)
        return _UnixJob(self._id, self._name, source, self._executable, self._args)


def create_job_builder() -> _JobBuilder:
    return _JobBuilder()


class _LegacyJobBuilder(_BaseJobBuilder):
    def __init__(self):
        super().__init__()
        self._ext_job = None
        self._script = None
        self._run_path = None
        self._num_cpu = None
        self._status_file = None
        self._ok_file = None
        self._exit_file = None
        self._done_callback_function = None
        self._exit_callback_function = None
        self._callback_arguments = None
        self._max_runtime = None
        self.reset()

    def reset(self):
        super().reset()
        self._ext_job = None
        self._script = None
        self._run_path = None
        self._num_cpu = None
        self._status_file = None
        self._ok_file = None
        self._exit_file = None
        self._done_callback_function = None
        self._exit_callback_function = None
        self._callback_arguments = None
        self._max_runtime = None
        return self

    def set_ext_job(self, ext_job):
        self._ext_job = ext_job
        return self

    def build(self):
        if self._id is None:
            self._id = str(uuid.uuid4())
        return _LegacyJob(self._id, self._name, self._ext_job)


def create_legacy_job_builder():
    return _LegacyJobBuilder()


class _BaseJob:
    def __init__(self, id_, name, source):
        if id_ is None:
            raise ValueError(f"{self} need id")
        if name is None:
            raise ValueError(f"{self} need name")
        self._id = id_
        self._name = name
        self._source = source

    def get_id(self):
        return self._id

    def get_name(self):
        return self._name

    def get_source(self, ee_id):
        return self._source.format(ee_id=ee_id)


class _UnixJob(_BaseJob):
    def __init__(
        self,
        id_,
        name,
        step_source,
        executable,
        args,
    ):
        super().__init__(id_, name, step_source)
        self._executable = executable
        self._args = args

    def get_executable(self):
        return self._executable

    def get_args(self):
        return self._args


class _FunctionJob(_BaseJob):
    def __init__(
        self,
        id_,
        name,
        step_source,
        command,
    ):
        super().__init__(id_, name, step_source)
        self._command = command

    def get_command(self):
        return self._command


class _LegacyJob(_BaseJob):
    def __init__(
        self,
        id_,
        name,
        ext_job,
    ):
        super().__init__(id_, name, "")  # no step_source needed for legacy (pt.)
        if ext_job is None:
            raise TypeError(f"{self} needs ext_job")
        self._ext_job = ext_job

    def get_ext_job(self):
        return self._ext_job


class _Stage:
    def __init__(
        self, id_: str, name: str, inputs: List[_IO], outputs: List[_IO]
    ) -> None:
        self._id = id_
        self._inputs = inputs
        self._outputs = outputs
        self._name = name

    def get_id(self):
        return self._id

    def get_inputs(self):
        return self._inputs

    def get_outputs(self):
        return self._outputs

    def get_name(self):
        return self._name


class _Step(_Stage):
    def __init__(self, id_, inputs, outputs, jobs, name, source):
        super().__init__(id_, name, inputs, outputs)
        if jobs is None:
            raise ValueError(f"{self} needs jobs")
        self._jobs = jobs
        self._source = source

    def get_jobs(self):
        return self._jobs

    def get_source(self, ee_id):
        return self._source.format(ee_id=ee_id)


class _UnixStep(_Step):
    def __init__(
        self,
        id_,
        inputs,
        outputs,
        jobs,
        name,
        source,
    ):
        super().__init__(id_, inputs, outputs, jobs, name, source)

    def get_task(self, output_transmitters, ee_id, *args, **kwargs):
        return UnixTask(self, output_transmitters, ee_id, *args, **kwargs)


class _FunctionStep(_Step):
    def __init__(
        self,
        id_,
        inputs,
        outputs,
        jobs,
        name,
        source,
    ):
        super().__init__(id_, inputs, outputs, jobs, name, source)

    def get_task(self, output_transmitters, ee_id, *args, **kwargs):
        return FunctionTask(self, output_transmitters, ee_id, *args, **kwargs)


class _LegacyStep(_Step):
    def __init__(
        self,
        id_,
        inputs,
        outputs,
        jobs,
        name,
        ee_url,
        source,
        max_runtime,
        callback_arguments,
        done_callback,
        exit_callback,
        num_cpu,
        run_path,
        job_script,
        job_name,
        run_arg,
    ):
        super().__init__(id_, inputs, outputs, jobs, name, source)
        if max_runtime is not None and max_runtime <= 0:
            raise ValueError(f"{self} needs positive max_runtime")
        if callback_arguments is None:
            raise ValueError(f"{self} needs callback_arguments")
        if done_callback is None:
            raise ValueError(f"{self} needs done_callback")
        if exit_callback is None:
            raise ValueError(f"{self} needs exit_callback")
        if num_cpu is None:
            raise ValueError(f"{self} needs num_cpu")
        if run_path is None:
            raise ValueError(f"{self} needs run_path")
        if job_script is None:
            raise ValueError(f"{self} needs job_script")
        if job_name is None:
            raise ValueError(f"{self} needs job_name")
        if run_arg is None:
            raise ValueError(f"{self} needs run_arg")
        self._max_runtime = max_runtime
        self._callback_arguments = callback_arguments
        self._done_callback = done_callback
        self._exit_callback = exit_callback
        self._num_cpu = num_cpu
        self._run_path = run_path
        self._job_script = job_script
        self._job_name = job_name
        self._run_arg = run_arg

    def get_max_runtime(self):
        return self._max_runtime

    def get_callback_arguments(self):
        return self._callback_arguments

    def get_done_callback(self):
        return self._done_callback

    def get_exit_callback(self):
        return self._exit_callback

    def get_num_cpu(self):
        return self._num_cpu

    def get_run_path(self):
        return self._run_path

    def get_job_script(self):
        return self._job_script

    def get_job_name(self):
        return self._job_name

    def get_run_arg(self):
        return self._run_arg


class _StageBuilder:
    def __init__(self):
        self._id: str = ""
        self._name: str = ""
        self._inputs: List[_IOBuilder] = []
        self._outputs: List[_IOBuilder] = []
        self._stages: List[_StageBuilder] = []

    def set_id(self, id_: str):
        self._id = id_
        return self

    def set_name(self, name: str):
        self._name = name
        return self

    def add_output(self, output: _IOBuilder):
        self._outputs.append(output)
        return self

    def add_input(self, input: _IOBuilder):
        self._inputs.append(input)
        return self

    def build(self):
        if not self._id:
            self._id = str(uuid.uuid4())
        if not self._name:
            raise ValueError(f"invalid name for stage {self._name}")
        inputs = [builder.build() for builder in self._inputs]
        outputs = [builder.build() for builder in self._outputs]
        return _Stage(self._id, self._name, inputs, outputs)

    def set_dummy_io(self):
        self.add_input(_DummyIOBuilder())
        self.add_output(_DummyIOBuilder())
        return self


class _StepBuilder(_StageBuilder):
    def __init__(self):
        super().__init__()
        self._jobs = []
        self._type = None
        self._parent_source = None

        # legacy parts
        self._max_runtime = None
        self._callback_arguments = None
        self._done_callback = None
        self._exit_callback = None
        self._num_cpu = None
        self._run_path = None
        self._job_script = None
        self._job_name = None
        self._run_arg = None

    def set_type(self, type_):
        self._type = type_
        return self

    def set_parent_source(self, source):
        self._parent_source = source
        return self

    def add_job(self, job: _JobBuilder) -> "_StepBuilder":
        self._jobs.append(job)
        return self

    def set_max_runtime(self, max_runtime):
        self._max_runtime = max_runtime
        return self

    def set_callback_arguments(self, callback_arguments):
        self._callback_arguments = callback_arguments
        return self

    def set_done_callback(self, done_callback):
        self._done_callback = done_callback
        return self

    def set_exit_callback(self, exit_callback):
        self._exit_callback = exit_callback
        return self

    def set_num_cpu(self, num_cpu):
        self._num_cpu = num_cpu
        return self

    def set_run_path(self, run_path):
        self._run_path = run_path
        return self

    def set_job_script(self, job_script):
        self._job_script = job_script
        return self

    def set_job_name(self, job_name):
        self._job_name = job_name
        return self

    def set_run_arg(self, run_arg):
        self._run_arg = run_arg
        return self

    def build(self):
        stage = super().build()

        step_source = self._parent_source + _SOURCE_TEMPLATE_STEP.format(
            step_id=stage.get_id()
        )
        source = _SOURCE_TEMPLATE_BASE + step_source

        jobs = [
            builder.set_parent_source(step_source).build() for builder in self._jobs
        ]
        if self._run_arg:
            return _LegacyStep(
                stage.get_id(),
                stage.get_inputs(),
                stage.get_outputs(),
                jobs,
                self._name,
                "",  # ee_url
                "",  # source
                self._max_runtime,
                self._callback_arguments,
                self._done_callback,
                self._exit_callback,
                self._num_cpu,
                self._run_path,
                self._job_script,
                self._job_name,
                self._run_arg,
            )
        if self._type == "function":
            return _FunctionStep(
                stage.get_id(),
                stage.get_inputs(),
                stage.get_outputs(),
                jobs,
                stage.get_name(),
                source,
            )
        elif self._type == "unix":
            return _UnixStep(
                stage.get_id(),
                stage.get_inputs(),
                stage.get_outputs(),
                jobs,
                stage.get_name(),
                source,
            )
        else:
            raise ValueError("Unexpected type while building step: {self._type}")


def create_step_builder() -> _StepBuilder:
    return _StepBuilder()


class _RealizationBuilder:
    def __init__(self):
        self._steps: List[_StepBuilder] = []
        self._stages: List[_StageBuilder] = []
        self._active = None
        self._iens = None

    def active(self, active) -> "_RealizationBuilder":
        self._active = active
        return self

    def add_step(self, step) -> "_RealizationBuilder":
        self._steps.append(step)
        return self

    def add_stage(self, stage: _StageBuilder) -> "_RealizationBuilder":
        self._stages.append(stage)
        return self

    def set_iens(self, iens) -> "_RealizationBuilder":
        self._iens = iens
        return self

    def build(self):
        realization_source = _SOURCE_TEMPLATE_REAL.format(iens=self._iens)
        source = _SOURCE_TEMPLATE_BASE + realization_source

        steps = [
            builder.set_parent_source(realization_source).build()
            for builder in self._steps
        ]

        ts_sorted_steps = _sort_steps(steps)

        return _Realization(
            self._iens,
            steps,
            self._active,
            source=source,
            ts_sorted_steps=ts_sorted_steps,
        )


def create_realization_builder() -> _RealizationBuilder:
    return _RealizationBuilder()


class _Realization:
    def __init__(self, iens, steps, active, source, ts_sorted_steps=None):
        if iens is None:
            raise ValueError(f"{self} needs iens")
        if steps is None:
            raise ValueError(f"{self} needs steps")
        if active is None:
            raise ValueError(f"{self} needs to be set either active or not")

        self._iens = iens
        self._steps = steps
        self._active = active

        self._source = source

        self._ts_sorted_indices = None
        if ts_sorted_steps is not None:
            self._ts_sorted_indices = list(range(0, len(ts_sorted_steps)))
            for idx, name in enumerate(ts_sorted_steps):
                for step_idx, step in enumerate(steps):
                    if step.get_name() == name:
                        self._ts_sorted_indices[idx] = step_idx
            if len(self._ts_sorted_indices) != len(steps):
                raise ValueError(
                    f"disparity between amount of sorted items ({self._ts_sorted_indices}) and steps, possibly duplicate step name?"
                )

    def get_steps(self):
        return self._steps

    def get_iens(self):
        return self._iens

    def is_active(self):
        return self._active

    def set_active(self, active):
        self._active = active

    def get_source(self, ee_id):
        return self._source.format(ee_id=ee_id)

    def get_steps_sorted_topologically(self) -> Iterator[_Step]:
        steps = self._steps
        if not self._ts_sorted_indices:
            raise NotImplementedError("steps were not sorted")
        for idx in self._ts_sorted_indices:
            yield steps[idx]


class _EnsembleBuilder:
    def __init__(self):
        self._reals = None
        self._forward_model = None
        self._size = None
        self._metadata = None
        self._legacy_dependencies = None
        self._inputs = None
        self._outputs = None

        self._custom_port_range = None
        self._max_running = None
        self._max_retries = None
        self._retry_delay = None
        self._executor = None

        self.reset()

    def reset(self) -> "_EnsembleBuilder":
        self._reals = []
        self._forward_model = None
        self._size = 0
        self._metadata = {}
        self._legacy_dependencies = None

        self._custom_port_range = None
        self._max_running = 10000
        self._max_retries = 0
        self._retry_delay = 5
        self._executor = "local"
        return self

    def set_forward_model(self, forward_model) -> "_EnsembleBuilder":
        if self._reals:
            raise ValueError(
                "Cannot set forward model when realizations are already specified"
            )
        self._forward_model = forward_model
        return self

    def add_realization(self, real) -> "_EnsembleBuilder":
        if self._forward_model:
            raise ValueError("Cannot add realization when forward model is specified")
        self._reals.append(real)
        return self

    def set_metadata(self, key, value) -> "_EnsembleBuilder":
        self._metadata[key] = value
        return self

    def set_ensemble_size(self, size) -> "_EnsembleBuilder":
        """Duplicate the ensemble members that existed at build time so as to
        get the desired state."""
        self._size = size
        return self

    def set_legacy_dependencies(self, *args) -> "_EnsembleBuilder":
        self._legacy_dependencies = args
        return self

    def set_inputs(self, inputs: InputMap) -> "_EnsembleBuilder":
        self._inputs = inputs.to_dict()
        return self

    def set_outputs(self, outputs: OutputMap) -> "_EnsembleBuilder":
        self._outputs = outputs.to_dict()
        return self

    def set_custom_port_range(self, custom_port_range) -> "_EnsembleBuilder":
        self._custom_port_range = custom_port_range
        return self

    def set_max_running(self, max_running) -> "_EnsembleBuilder":
        self._max_running = max_running
        return self

    def set_max_retries(self, max_retries) -> "_EnsembleBuilder":
        self._max_retries = max_retries
        return self

    def set_retry_delay(self, retry_delay) -> "_EnsembleBuilder":
        self._retry_delay = retry_delay
        return self

    def set_executor(self, executor) -> "_EnsembleBuilder":
        self._executor = executor
        return self

    @staticmethod
    def from_legacy(
        run_context,
        forward_model,
        queue_config,
        analysis_config,
        res_config,
    ) -> "_EnsembleBuilder":
        builder = _EnsembleBuilder().set_legacy_dependencies(
            queue_config,
            analysis_config,
        )

        num_cpu = res_config.queue_config.num_cpu
        if num_cpu == 0:
            num_cpu = res_config.ecl_config.num_cpu

        max_runtime = analysis_config.get_max_runtime()
        if max_runtime == 0:
            max_runtime = None

        for iens in range(0, len(run_context)):
            active = run_context.is_active(iens)
            real = create_realization_builder().set_iens(iens).active(active)
            step = (
                create_step_builder().set_id("0").set_dummy_io().set_name("legacy step")
            )
            if active:
                real.active(True).add_step(step)
                for index in range(0, len(forward_model)):
                    ext_job = forward_model.iget_job(index)
                    step.add_job(
                        create_legacy_job_builder()
                        .set_id(index)
                        .set_name(ext_job.name())
                        .set_ext_job(ext_job)
                    )
                run_arg = run_context[iens]
                step.set_max_runtime(max_runtime).set_callback_arguments(
                    [run_arg, res_config]
                ).set_done_callback(
                    EnKFState.forward_model_ok_callback
                ).set_exit_callback(
                    EnKFState.forward_model_exit_callback
                ).set_num_cpu(
                    num_cpu
                ).set_run_path(
                    run_arg.runpath
                ).set_job_script(
                    res_config.queue_config.job_script
                ).set_job_name(
                    run_arg.job_name
                ).set_run_arg(
                    run_arg
                )
            builder.add_realization(real)
        return builder

    def _build_io_maps(self, reals: List[_RealizationBuilder]) -> None:
        i_matrix: Dict[int, Dict[str, Optional["ert.data.RecordTransmitter"]]] = {
            i: {} for i in range(len(reals))
        }
        o_matrix: Dict[int, Dict[str, Optional["ert.data.RecordTransmitter"]]] = {
            i: {} for i in range(len(reals))
        }
        o_names = set()
        for iens, real in enumerate(reals):
            for step in real._steps:
                for output in step._outputs:
                    if isinstance(output, _DummyIOBuilder):
                        continue
                    factory = output.transmitter_factory(iens)
                    o_matrix[iens][output._name] = factory() if factory else None
                    o_names.add(output._name)
        for iens, real in enumerate(reals):
            for step in real._steps:
                for input_ in step._inputs:
                    if isinstance(input_, _DummyIOBuilder):
                        continue
                    if input_._name not in o_names:
                        factory = input_.transmitter_factory(iens)
                        i_matrix[iens][input_._name] = factory() if factory else None
        self._inputs = InputMap.from_dict(i_matrix).validate().to_dict()
        self._outputs = OutputMap.from_dict(o_matrix).validate().to_dict()

    def build(self) -> _Ensemble:
        if not (self._reals or self._forward_model):
            raise ValueError("Either forward model or realizations needs to be set")

        reals = []
        if self._forward_model:
            # duplicate the original forward model into realizations
            for i in range(self._size):
                logger.debug(f"made deep-copied real {i}")
                real = copy.deepcopy(self._forward_model)
                real.set_iens(i)
                reals.append(real)
        else:
            reals = self._reals

        # legacy has dummy IO, so no need to build an IO map
        if not self._legacy_dependencies:
            self._build_io_maps(reals)

        reals = [builder.build() for builder in reals]

        if self._legacy_dependencies:
            return _LegacyEnsemble(reals, self._metadata, *self._legacy_dependencies)
        else:
            return PrefectEnsemble(
                reals=reals,
                inputs=self._inputs,
                outputs=self._outputs,
                max_running=self._max_running,
                max_retries=self._max_retries,
                executor=self._executor,
                retry_delay=self._retry_delay,
                custom_port_range=self._custom_port_range,
            )


def create_ensemble_builder() -> _EnsembleBuilder:
    return _EnsembleBuilder()


def create_ensemble_builder_from_legacy(
    run_context,
    forward_model,
    queue_config,
    analysis_config,
    res_config,
):
    return _EnsembleBuilder.from_legacy(
        run_context,
        forward_model,
        queue_config,
        analysis_config,
        res_config,
    )


StepBuilder = _StepBuilder
Step = _Step
