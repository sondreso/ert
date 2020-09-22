#!/bin/env python
import os
import sys
import click
import numpy as np
import pandas as pd

from SEIR.parser.config_file_parser import parse_config_ini
from SEIR.seir import SEIR
from SEIR.visualization import visualize_seir_computation

WD = os.path.dirname(__file__)


@click.command()
@click.option('--config_file', '-cf',
              type=click.Path(exists=True),
              help='Path to config ini file.')
@click.option('--contacts_matrix_file', '-cm',
              type=click.Path(exists=True),
              help='Path to contact matrix file')
@click.option('--visualize-compartments', '-vc', default=True,
              type=bool,
              is_flag=True)
def main(config_file, contacts_matrix_file, visualize_compartments):
    """Console script for SEIR."""
    # TODO: Handle somehow the creation of an imports function
    # Setup the model
    if not config_file:
        config_file = f'{WD}/model_configs/finland'
    kwargs, initial_state_kwargs, sim_kwargs, restr_info = parse_config_ini(config_file)

    if contacts_matrix_file:
        with open(contacts_matrix_file) as contacts_matrix_file:
            kwargs["contacts_matrix"] = np.loadtxt(contacts_matrix_file)

    results = pd.read_csv('out.csv')
    # Visualize the results
    visualize_seir_computation(
        results,
        compartments=kwargs['compartments'],
        restrictions_info=restr_info,
        show_individual_compartments=visualize_compartments)

if __name__ == "__main__":
    main()