from typing import List, Optional

from pydantic import BaseModel, ValidationError, create_model

import ert3


def foo():
    print("LOL")


def main():
    plugin_registry = ert3.config.ConfigPluginRegistry()
    plugin_registry.register_category(category="transformation")
    plugin_manager = ert3.plugins.ErtPluginManager()
    plugin_manager.get_plugin_configs(registry=plugin_registry)

    stages_config = ert3.config.get_configs(plugin_registry=plugin_registry)

    stages_configs = [
        {
            "__root__": (
                {
                    "name": "step",
                    "input": [
                        {
                            "name": "test",
                            "transformation": {
                                "type": "serialization",
                                "location": "test.json",
                            },
                        }
                    ],
                    "output": [
                        {
                            "name": "foo",
                            "transformation": {
                                "type": "serialization",
                                "location": "test.json",
                            },
                        }
                    ],
                    "function": "ert3.console:main",
                },
            )
        },
        # {
        #     "input": {
        #         "name": "test",
        #         "transformation": {"type": "directory", "location": "test/hei"},
        #     },
        # },
        # {
        #     "input": {
        #         "name": "test",
        #         "transformation": {"type": "file", "mime": "hei"},
        #     },
        # },
        # {
        #     "input": {
        #         "name": "test",
        #         "transformation": {
        #             "type": "summary",
        #             "location": "test.smry",
        #             "smry_keys": ["WOPR", "FOPR"],
        #         },
        #     },
        # },
    ]

    for config in stages_configs:
        print(f"Attempting to validate: {config}")
        try:
            stage = stages_config(**config)
            print(stage)
            for step in stage:
                print(step)
                for input_ in step.input:
                    print(step.input[input_].get_transformation_instance())
        except ValidationError as e:
            print(f"Error: {e}")
        print()


"""
$ python test.py
Attempting to validate: {'input': {'name': 'test', 'transformation': {'type': 'file', 'location': 'test.json'}}}
input=StageIO(name='test', transformation=FullFileTransformationConfig(location='test.json', mime='', type='file'))
DummyInstance(config=location='test.json' mime='' type='file')

Attempting to validate: {'input': {'name': 'test', 'transformation': {'type': 'directory', 'location': 'test/hei'}}}
input=StageIO(name='test', transformation=FullDirectoryTransformationConfig(location='test/hei', type='directory'))
DummyInstance(config=location='test/hei' type='directory')

Attempting to validate: {'input': {'name': 'test', 'transformation': {'type': 'file', 'mime': 'hei'}}}
Error: 1 validation error for Stage
input -> transformation -> FullFileTransformationConfig -> location
  field required (type=value_error.missing)

Attempting to validate: {'input': {'name': 'test', 'transformation': {'type': 'summary', 'location': 'test.smry', 'smry_keys': ['WOPR', 'FOPR']}}}
input=StageIO(name='test', transformation=FullSummaryTransformationConfig(location='test.smry', smry_keys=['WOPR', 'FOPR'], type='summary'))
DummyInstance(config=location='test.smry' smry_keys=['WOPR', 'FOPR'] type='summary')
"""

if __name__ == "__main__":
    main()
