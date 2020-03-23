from ert_shared.storage.rdb_api import RdbApi
from ert_shared.storage.blob_api import BlobApi


def get_data(id, blob_api=None):
    if blob_api is None:
        blob_api = BlobApi()

    with blob_api:
        return blob_api.get_blob(id).data


def get_ensemble(name, rdb_api=None):
    if rdb_api is None:
        rdb_api = RdbApi()

    ensemble = rdb_api.get_ensemble(name=name)
    ensemble_dict = dict()
    ensemble_dict["name"] = ensemble.name
    ensemble_dict["parameter_definitions"] = [
        {"name": definition.name, "group": definition.group}
        for definition in ensemble.parameter_definitions
    ]
    ensemble_dict["response_definitions"] = [
        {"name": definition.name, "indexes_ref": definition.indexes_ref}
        for definition in ensemble.response_definitions
    ]
    ensemble_dict["realizations"] = [
        {
            "index": realization.index,
            "responses": [
                {
                    "name": response.response_definition.name,
                    "values_ref": response.values_ref,
                }
                for response in realization.responses
            ],
            "parameters": [
                {
                    "name": parameter.parameter_definition.name,
                    "group": parameter.parameter_definition.group,
                    "value_ref": parameter.value_ref,
                }
                for parameter in realization.parameters
            ],
        }
        for realization in ensemble.realizations
    ]

    return ensemble_dict


def get_all_ensembles(rdb_api=None):
    if rdb_api is None:
        rdb_api = RdbApi()

    with rdb_api:
        return [ensemble.name for ensemble in rdb_api.get_all_ensembles()]
