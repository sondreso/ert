import ert3

import dummy_plugins


def test_no_plugins(plugin_registry):
    assert plugin_registry.get_factory("transformation", "serialization")
    assert plugin_registry.get_factory("transformation", "summary")
    assert plugin_registry.get_factory("transformation", "directory")

    assert plugin_registry.get_descriminator("transformation") == "type"

    generated_configs = [
        "ert3.config._config_plugin_registry.FullSerializationTransformationConfig",
        "ert3.config._config_plugin_registry.FullSummaryTransformationConfig",
        "ert3.config._config_plugin_registry.FullDirectoryTransformationConfig",
    ]
    assert (
        str(plugin_registry.get_type("transformation"))
        == f"typing.Union[{', '.join(generated_configs)}]"
    )

    field = plugin_registry.get_field("transformation")
    assert field.discriminator == "type"


def test_dummy_plugin():
    plugin_registry = ert3.config.ConfigPluginRegistry()
    plugin_registry.register_category(category="test", descriminator="type")
    plugin_manager = ert3.plugins.ErtPluginManager(plugins=[dummy_plugins])
    plugin_manager.collect(registry=plugin_registry)

    assert plugin_registry.get_factory("test", "foo")
    assert (
        str(plugin_registry.get_type("test"))
        == "<class 'ert3.config._config_plugin_registry.Full_FooConfig'>"
    )
    assert not plugin_registry.get_field("test").discriminator
