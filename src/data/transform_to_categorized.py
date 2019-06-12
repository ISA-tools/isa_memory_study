# -*- coding: utf-8 -*-
import click
import logging
from pathlib import Path
from dotenv import find_dotenv, load_dotenv
import pandas as pd
import numpy as np


@click.command()
@click.argument('input_file', type=click.File('r'))
@click.argument('output_file', type=click.File('w'))
def main(input_file, output_file):
    """ Transforms the processed object footprint data into a new table with
        categories for each record.
    """
    logger = logging.getLogger(__name__)
    logger.info('transforming dataset to categorized table')
    df = pd.read_csv(input_file)
    # put the records by category to help with downstream processing
    disk_sizes = []
    obj_sizes = []
    categories = []
    mtblsids = []

    for _, row in df.iterrows():
        disk_sizes.append(row.disk_size)
        obj_sizes.append(row.df_size)
        mtblsids.append(row.mtblsid)
        categories.append('DataFrame')
        disk_sizes.append(row.disk_size)
        obj_sizes.append(row.isa_size)
        mtblsids.append(row.mtblsid)
        if '_s' in row.mtblsid:
            categories.append('CisaFrame')
        elif '_a' in row.mtblsid:
            categories.append('CisaFrame')   

    df_by_cat = pd.DataFrame({
        'disk_size': disk_sizes,
        'size': obj_sizes,
        'log_size': np.log(obj_sizes),
        'category': categories,
        'mtblsid' : mtblsids
    })

    df_sorted = df_by_cat.sort_values(by='category')
    df_sorted.to_csv(output_file, index=None)


if __name__ == '__main__':
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_fmt)

    # not used in this stub but often useful for finding various files
    project_dir = Path(__file__).resolve().parents[2]

    # find .env automagically by walking up directories until it's found, then
    # load up the .env entries as environment variables
    load_dotenv(find_dotenv())

    main()
