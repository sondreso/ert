from typing import Any, Type
import ert3
from pydantic import BaseModel


class _FooConfig(BaseModel):
    key: str


class _Foo:
    def __init__(self, key: str) -> None:
        self.key = key


def _factory(config: Type[BaseModel]) -> Any:
    if isinstance(config, _FooConfig):
        return _Foo(key=config.key)


@ert3.plugins.plugin_manager.hook_implementation
def configs(registry: ert3.config.ConfigPluginRegistry) -> None:
    registry.register(
        name="foo",
        category="test",
        config=_FooConfig,
        factory=_factory,
    )
