from functools import partialmethod
import sys
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple, Type, no_type_check

from pydantic import BaseModel, ValidationError, create_model, root_validator, validator

import ert
from ._config_plugin_registry import ConfigPluginRegistry, getter_template

if sys.version_info >= (3, 8):
    from typing import Literal
else:
    from typing_extensions import Literal


class SourceNS(str, Enum):
    stochastic = "stochastic"
    storage = "storage"
    resources = "resources"


class _EnsembleConfig(BaseModel):
    class Config:
        validate_all = True
        validate_assignment = True
        extra = "forbid"
        allow_mutation = False
        arbitrary_types_allowed = True
        underscore_attrs_are_private = True


class ForwardModel(_EnsembleConfig):
    stage: str
    driver: Literal["local", "pbs"] = "local"


def _ensure_transformation_for_resources(
    cls, v: Optional[Type[BaseModel]], values: Dict[str, Any]
):
    if "source" not in values:
        return v
    namespace, _ = values["source"].split(".", maxsplit=1)
    if namespace == SourceNS.resources and not v:
        raise ValueError(f"need transformation for source '{values['source']}'")
    return v


class Input(_EnsembleConfig):
    _namespace: SourceNS
    _location: str
    record: str
    source: str

    @no_type_check
    def __init__(self, **data: Any) -> None:
        super().__init__(**data)
        if "source" not in data:
            raise RuntimeError(
                "The input configuration must be obtained via 'create_ensemble_config'"
            )
        parts = data["source"].split(".", maxsplit=1)
        self._namespace = SourceNS(parts[0])
        self._location = parts[1]

    @validator("source")
    def _ensure_source_format(cls, v: str) -> str:
        parts = v.split(".", maxsplit=1)
        if not len(parts) == 2:
            raise ValueError(f"{v} missing at least one dot (.) to form a namespace")
        return v

    @root_validator(pre=True)
    def _inject_location(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        """
        'location' will be defined as whatever comes after 'resources.' in
        the 'source' value. This is injected into a transformation should it
        exist.
        """
        if "transformation" not in values or "source" not in values:
            return values
        source = cls._ensure_source_format(values["source"])
        _, location = source.split(".", maxsplit=1)
        values["transformation"]["location"] = location
        return values

    @property
    def source_namespace(self) -> SourceNS:
        return self._namespace

    @property
    def source_location(self) -> str:
        return self._location

    @property
    def direction(self):
        # We know, since this is _input_, that we're always going in the
        # TO_RECORD direction.
        return ert.data.RecordTransformationDirectionality.TO_RECORD


class Output(_EnsembleConfig):
    record: str


class EnsembleConfig(_EnsembleConfig):
    forward_model: ForwardModel
    input: Tuple[Input, ...]
    output: Tuple[Output, ...]
    size: Optional[int] = None
    storage_type: str = "ert_storage"


def create_ensemble_config(
    plugin_registry: ConfigPluginRegistry,
) -> Type[EnsembleConfig]:
    transformation = plugin_registry.get_field("transformation")
    transformation_type = plugin_registry.get_type("transformation")
    is_optional = transformation.default != Ellipsis
    input_fields: Dict[str, Any] = {
        "transformation": (transformation_type, transformation),
    }

    input_config = create_model(
        "PluggedInput",
        __base__=Input,
        __module__=__name__,
        __validators__={
            "ensure_transformation_for_resource": validator(
                "transformation", allow_reuse=True
            )(_ensure_transformation_for_resources),
        },
        **input_fields,
    )

    setattr(
        input_config,
        "get_transformation_instance",
        partialmethod(
            getter_template,
            category="transformation",
            optional=is_optional,
            plugin_registry=plugin_registry,
        ),
    )

    ensemble_config = create_model(
        "PluggedEnsembleConfig",
        __base__=EnsembleConfig,
        __module__=__name__,
        input=(Tuple[input_config, ...], ...),
    )

    return ensemble_config


def load_ensemble_config(
    config_dict: Dict[str, Any], plugin_registry: ConfigPluginRegistry
) -> EnsembleConfig:
    try:
        ensemble_config = create_ensemble_config(plugin_registry=plugin_registry)
        return ensemble_config.parse_obj(config_dict)
    except ValidationError as err:
        raise ert.exceptions.ConfigValidationError(str(err), source="ensemble")
