#!/bin/env python
import os
import sys
import click
import numpy as np

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
@click.option('--visualize-compartments', '-vc',
              type=bool,
              is_flag=True)
@click.option('--output_file_format', '-of',
              type=click.Choice(['json', 'csv']), default='csv',
              help='Output file format')
@click.option('--output_file', '-o', default="out.csv",
              type=str,
              help='Path to output file.')
def main(config_file, contacts_matrix_file, visualize_compartments, output_file_format, output_file):
    """Console script for SEIR."""
    # TODO: Handle somehow the creation of an imports function
    # Setup the model
    if not config_file:
        sys.exit("No config file!")
    kwargs, initial_state_kwargs, sim_kwargs, restr_info = parse_config_ini(config_file)

    if contacts_matrix_file:
        with open(contacts_matrix_file) as contacts_matrix_file:
            kwargs["contacts_matrix"] = np.loadtxt(contacts_matrix_file)

    model = SEIR(**kwargs)

    # Setup initial state
    model.set_initial_state(**initial_state_kwargs)

    # Simulate up to 200 days
    model.simulate(**sim_kwargs)

    # Evaluate the solution
    time = np.arange(0, sim_kwargs['max_simulation_time'], 1, dtype=int)
    results = model.evaluate_solution(time)
    # Save data
    if output_file_format == 'csv':
        output_file = output_file or os.path.basename(f'{config_file}.csv')
        results.to_csv(output_file)
    elif output_file_format == 'json':
        output_file = output_file or os.path.basename(f'{config_file}.json')
        results.to_json(output_file)

    hospitalized = results["hospitalized (active)"].fillna(0).values.tolist()
    def convert_num(a):
        return str(int(round(a)))
    with open('hospitalized_0.out', 'w') as f:
        f.write('\n'.join(map(convert_num, hospitalized)))

if __name__ == "__main__":
    main()