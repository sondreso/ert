import importlib.util
from collections import OrderedDict
from functools import partialmethod
from importlib.abc import Loader
from types import MappingProxyType
from typing import (
    Any,
    Callable,
    Dict,
    List,
    Mapping,
    Tuple,
    Union,
    cast,
    Type,
)

from pydantic import (
    BaseModel,
    FilePath,
    ValidationError,
    create_model,
    root_validator,
    validator,
)


import ert
from ._config_plugin_registry import ConfigPluginRegistry, getter_template

from ._validator import ensure_mime


def _import_from(path: str) -> Callable[..., Any]:
    if ":" not in path:
        raise ValueError("Function should be defined as module:function")
    module_str, func = path.split(":")
    spec = importlib.util.find_spec(module_str)
    if spec is None:
        raise ModuleNotFoundError(f"No module named '{module_str}'")
    module = importlib.util.module_from_spec(spec)
    # A loader should always have been set, and it is assumed it a PEP 302
    # compliant Loader. A cast is made to make this clear to the typing system.
    cast(Loader, spec.loader).exec_module(module)
    try:
        return cast(Callable[..., Any], getattr(module, func))
    except AttributeError:
        raise ImportError(name=func, path=module_str)


class _StagesConfig(BaseModel):
    class Config:
        validate_all = True
        validate_assignment = True
        extra = "forbid"
        allow_mutation = False
        arbitrary_types_allowed = True


class TransportableCommand(_StagesConfig):
    name: str
    location: FilePath
    mime: str = ""

    _ensure_transportablecommand_mime = validator("mime", allow_reuse=True)(
        ensure_mime("location")
    )


# mypy ignore missing parameter for generic type
class IndexedOrderedDict(OrderedDict):  # type: ignore
    """Extend OrderedDict to add support for accessing elements by their index."""

    def __getitem__(self, attr: Union[str, int]) -> Any:
        if isinstance(attr, str):
            return super().__getitem__(attr)
        return self[list(self.keys())[attr]]


def _valid_direction(cls, v: str) -> ert.data.RecordTransformationDirectionality:
    return ert.data.RecordTransformationDirectionality.from_direction(v)


def create_stage_io(plugin_registry: ConfigPluginRegistry) -> Type[BaseModel]:
    transformation = plugin_registry.get_field("transformation")
    transformation_type = plugin_registry.get_type("transformation")
    is_optional = transformation.default != Ellipsis
    fields: Dict[str, Any] = {
        "name": (str, None),
        "direction": (ert.data.RecordTransformationDirectionality, ...),
        "transformation": (transformation_type, transformation),
    }

    stage_io = create_model(
        "StageIO",
        __base__=_StagesConfig,
        __module__=__name__,
        __validators__={
            "valid_direction": validator(
                "direction", pre=True, always=True, allow_reuse=True
            )(_valid_direction)
        },
        **fields,
    )

    setattr(
        stage_io,
        "get_transformation_instance",
        partialmethod(
            getter_template,
            category="transformation",
            optional=is_optional,
            plugin_registry=plugin_registry,
        ),
    )

    return stage_io


def create_stages_config(plugin_registry: ConfigPluginRegistry) -> "Type[StagesConfig]":
    """
    We now need the create configs dynamically,
    as we would like to control the schema created based on the plugins we provide.
    This will allow us to specify a subset of plugins we want to have effect at runtime,
    such as only using configs from ert in tests.
    """
    StageIO = create_stage_io(plugin_registry=plugin_registry)

    # duck punching _Step to bridge static and dynamic config definitions. StageIO
    # exists only at run-time, but we'd like _Step (and subclasses) to be static.
    setattr(_Step, "_stageio_cls", StageIO)

    # Returning the StagesConfig class to underline that it needs some dynamic mutation,
    # and allows us to change to fully dynamic creation later on.
    return StagesConfig


def _create_io_mapping(
    cls,
    ios: List[Dict[str, str]],
    direction: str,
) -> Mapping[str, Type[_StagesConfig]]:
    if not cls._stageio_cls:
        raise RuntimeError(
            "Step configuration must be obtained through 'create_stages_config'."
        )

    for io in ios:
        if "direction" not in io:
            io["direction"] = direction

    ordered_dict = IndexedOrderedDict(
        {io["name"]: cls._stageio_cls(**io) for io in ios}
    )

    proxy = MappingProxyType(ordered_dict)
    return proxy


class _Step(_StagesConfig):
    name: str
    input: MappingProxyType  # type: ignore
    output: MappingProxyType  # type: ignore

    @validator("input", pre=True, always=True, allow_reuse=True)
    def _create_input_mapping(cls, ios: List[Dict[str, str]]):
        return _create_io_mapping(cls, ios, direction="input")

    @validator("output", pre=True, always=True, allow_reuse=True)
    def _create_output_mapping(cls, ios: List[Dict[str, str]]):
        return _create_io_mapping(cls, ios, direction="output")


class Function(_Step):
    function: Callable  # type: ignore

    @validator("function", pre=True)
    def function_is_callable(cls, value) -> Callable:  # type: ignore
        return _import_from(value)


class Unix(_Step):
    script: Tuple[str, ...]
    transportable_commands: Tuple[TransportableCommand, ...]

    @root_validator
    def ensure_ios_has_transformation(cls, values):
        for io in ("input", "output"):
            if io not in values:
                continue
            for name, io_ in values[io].items():
                if not io_.transformation:
                    raise ValueError(f"io '{name}' had no transformation")
        return values


Step = Union[Function, Unix]


class StagesConfig(BaseModel):
    __root__: Tuple[Union[Function, Unix], ...]

    def step_from_key(self, key: str) -> Union[Function, Unix, None]:
        return next((step for step in self if step.name == key), None)

    def __iter__(self):  # type: ignore
        return iter(self.__root__)

    def __getitem__(self, item):  # type: ignore
        return self.__root__[item]

    def __len__(self):  # type: ignore
        return len(self.__root__)


def load_stages_config(
    config_dict: Dict[str, Any], plugin_registry: ConfigPluginRegistry
) -> StagesConfig:
    stages_config = create_stages_config(plugin_registry=plugin_registry)
    try:
        return stages_config.parse_obj(config_dict)
    except ValidationError as err:
        raise ert.exceptions.ConfigValidationError(str(err), source="stages")
