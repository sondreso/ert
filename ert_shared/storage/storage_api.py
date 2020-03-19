from ert_shared.storage.rdb_api import RdbApi


def get_response_data(name, ensemble_name, rdb_api=None):
    if rdb_api is None:
        rdb_api = RdbApi()

    with rdb_api:
        for response in rdb_api.get_response_data(name, ensemble_name):
            yield response

<<<<<<< HEAD
def get_all_ensembles(rdb_api=None):
    if rdb_api is None:
        rdb_api = RdbApi()
=======

def get_parameters(ensemble_name, repository=None):
    if repository is None:
        repository = ErtRepository()

    with repository:
        ensemble = repository.get_ensemble(name=ensemble_name)
        return [{"name": definition.name, "group": definition.group} for definition in ensemble.parameter_definitions]

def get_parameter_data(name, group, ensemble_name, repository=None):
    if repository is None:
        repository = ErtRepository()

    with repository:
        for response in repository.get_parameter_data(name, group, ensemble_name):
            yield response

def get_all_ensembles(repository=None):
    if repository is None:
        repository = ErtRepository()
>>>>>>> 9d97ef6... tmp: expose parameters

    with rdb_api:
        return [ensemble.name for ensemble in rdb_api.get_all_ensembles()]
