import importlib
from collections import defaultdict
from typing import (
    Any,
    Callable,
    DefaultDict,
    Dict,
    List,
    NamedTuple,
    Optional,
    Type,
    Union,
)
from typing_extensions import Literal

from pydantic import BaseModel, Field, create_model


class _RegisteredConfig(NamedTuple):
    config: Type[BaseModel]
    factory: Callable[[Type[BaseModel]], Any]


class ConfigPluginRegistry:
    def __init__(self) -> None:
        self._descriminator: Dict[str, str] = {}
        self._registry: Dict[str, Dict[str, _RegisteredConfig]] = {}
        self._default: DefaultDict[str, Any] = defaultdict(lambda: Ellipsis)

    def register_category(
        self,
        category: str,
        descriminator: str = "type",
        optional: bool = True,
    ):
        if category in self._registry:
            raise ValueError(f"Category '{category}' is already registered")
        self._descriminator[category] = descriminator
        self._registry[category] = {}

        if optional:
            self._default[category] = None

    def register(
        self,
        name: str,
        category: str,
        config: Type[BaseModel],
        factory: Callable[[Type[BaseModel], Type[BaseModel]], Any],
    ):
        if not category in self._registry:
            raise ValueError(
                f"Unknown category '{category}' when registering plugin config '{name}'"
            )
        if name in self._registry[category]:
            raise ValueError(f"{name} is already registered")

        field_definitions: Any = {self._descriminator[category]: (Literal[name], ...)}  # type: ignore
        config_name = f"Full{config.__name__}"
        full_config = create_model(
            config_name, __base__=config, __module__=__name__, **field_definitions
        )

        # make importable
        mod = importlib.import_module(__name__)
        setattr(mod, config_name, full_config)

        self._registry[category][name] = _RegisteredConfig(
            config=full_config, factory=factory
        )

    def get_factory(self, category: str, name: str):
        return self._registry[category][name].factory

    def get_descriminator(self, category: str):
        return self._descriminator[category]

    def get_type(self, category: str):
        if not category in self._registry:
            raise ValueError(f"Unknown category '{category}'")
        values = tuple(o.config for o in self._registry[category].values())
        if not values:
            raise ValueError(
                f"Using a plugin field requires at least one registered type, category '{category}' has no registered plugins"
            )

        if len(values) > 1:
            return Union[values]  # type: ignore
        else:
            return values[0]

    def get_field(self, category: str):
        if not category in self._registry:
            raise ValueError(f"Unknown category '{category}'")
        if not self._registry[category]:
            raise ValueError(
                f"Using a plugin field requires at least one registered type, category '{category}' has no registered plugins"
            )

        if len(self._registry[category]) == 1:
            return Field(self._default[category])
        else:
            return Field(
                self._default[category], discriminator=self._descriminator[category]
            )


def getter_template(
    self, category: str, optional: bool, plugin_registry: ConfigPluginRegistry
):
    config_instance = getattr(self, category)
    if optional and not config_instance:
        return None
    elif not optional and not config_instance:
        raise ValueError("no config, but was required for '{category}' configuration")
    descriminator_value = getattr(
        config_instance, plugin_registry.get_descriminator(category=category)
    )
    return plugin_registry.get_factory(category=category, name=descriminator_value)(
        config_instance, parent_config=self
    )
