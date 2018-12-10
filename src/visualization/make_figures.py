# -*- coding: utf-8 -*-
import click
import logging
from pathlib import Path
from dotenv import find_dotenv, load_dotenv
import pandas as pd
from os.path import join
import logging
import matplotlib.pyplot as plt
import numpy as np


@click.command()
@click.argument('input_file', type=click.File('r'))
@click.argument('output_filepath', type=click.Path())
def main(input_file, output_filepath):
    """ Runs data visualization scripts to turn raw data from data/processed
        into charts for figures (saved in reports/figures).

        We load the object footprint data and create several figures:
            - disk size of table files vs. in-memory size of DataFrames and
              ISA objects
            - disk size of table files vs. log2 in-memory size of DataFrames and
              ISA objects
            - line best fits using linear regression to disk sizes vs. mem-sizes
    """
    logger = logging.getLogger(__name__)
    logger.info('making figures from processed data')

    df = pd.read_csv(input_file)

    def plot_single_category(df, category, colour):
        fig, ax = plt.subplots()
        fig.suptitle('Disk vs. {} objects'.format(category), fontsize=12,
                     fontweight='bold')
        # print("before: ", df)
        df = df[df.category == category].reset_index()
        # print("after: ", df)
        identifiers = df['mtblsid']
        # disk_sizes = df['disk_size'] / 1024
        disk_sizes = df.disk_size / 1024
        mem_sizes = df['size'] / 1024
        # mem_sizes = df.size / 1024
        ax.plot(disk_sizes, mem_sizes, 'o', c=colour, alpha=0.25)
        ax.set_xlabel('Disk size (kb)')
        ax.set_ylabel('Memory size (kb)')
        
        for i, txt in enumerate(identifiers):
            # print("label:",str(txt))
            # plt.text(x+0.3, y+0.3, txt, fontsize=9)
            ax.annotate(txt, (disk_sizes.loc[i], mem_sizes.loc[i]), size=10)

        # for k, v in df.category.iterrows():
        #     print(k,v)
        #     ax.annotate(k, v,
        #         xytext=(10,-5), textcoords='offset points',
        #         family='sans-serif', fontsize=18, color='darkslategrey')

        return fig, ax

    max_disk_size = 100000
    df_max_filtered = df[df.disk_size < max_disk_size]
    print("df_max_filtered:", df_max_filtered)

    # plot scatterplot for ISA objects only
    logger.info('Saving scatter_isa_only figures (pdf, png)')
    fig_isa_only, _ = plot_single_category(df_max_filtered, 'ISA', 'orange')
    fig_isa_only.savefig(join(output_filepath, 'scatter_isa_only.pdf'))
    fig_isa_only.savefig(join(output_filepath, 'scatter_isa_only.png'))

    # plot scatterplot for DataFrame objects only
    logger.info('Saving scatter_df_only figures (pdf, png)')
    fig_df_only, _ = plot_single_category(df_max_filtered, 'DataFrame', 'green')
    fig_df_only.savefig(join(output_filepath, 'scatter_df_only.pdf'))
    fig_df_only.savefig(join(output_filepath, 'scatter_df_only.png'))




    # plot scatterplot for both categories
    max_disk_size = 100000
    df_max_filtered = df[df.disk_size < max_disk_size]

    logger.info('Saving scatter figure (pdf, png)')
    fig_scatter, ax_scatter = plot_single_category(df_max_filtered, 'DataFrame',
                                                   'green')
    fig_scatter.suptitle('Disk vs. DataFrame and ISA objects', fontsize=12,
                 fontweight='bold')
    df_isa = df_max_filtered[df_max_filtered.category == 'ISA']
    disk_sizes = df_isa.disk_size / 1024
    mem_sizes = df_isa['size'] / 1024
    ax_scatter.plot(disk_sizes, mem_sizes, 'o', c='orange', alpha=0.25)
    ax_scatter.set_xlabel('Disk size (kb)')
    ax_scatter.set_ylabel('Memory size (kb)')
    fig_scatter.savefig(join(output_filepath, 'scatter.pdf'))
    fig_scatter.savefig(join(output_filepath, 'scatter.png'))

    # plot log scatterplot for both categories
    logger.info('Saving log scatter figure (pdf, png)')
    fig_log_scatter, ax_log_scatter = plt.subplots()
    fig_log_scatter.suptitle('Disk vs. DataFrame and ISA objects', fontsize=12,
                             fontweight='bold')
    df_df = df_max_filtered[df_max_filtered.category == 'DataFrame']
    disk_sizes = df_df.disk_size / 1024
    log_mem_sizes = df_df.log_size / 1024
    ax_log_scatter.plot(disk_sizes, log_mem_sizes, 'o', c='green', alpha=0.25)
    ax_log_scatter.set_xlabel('Disk size (kb)')
    ax_log_scatter.set_ylabel('Log2 Memory size (kb)')
    df_isa = df_max_filtered[df_max_filtered.category == 'ISA']
    disk_sizes = df_isa.disk_size / 1024
    log_mem_sizes = df_isa.log_size / 1024
    ax_log_scatter.plot(disk_sizes, log_mem_sizes, 'o', c='orange', alpha=0.25)
    ax_log_scatter.set_xlabel('Disk size (kb)')
    ax_log_scatter.set_ylabel('Log2 Memory size (kb)')
    fig_log_scatter.savefig(join(output_filepath, 'log_scatter.pdf'))
    fig_log_scatter.savefig(join(output_filepath, 'log_scatter.png'))

    def plot_single_category_fit(df, category, dot_colour, line_colour):
        f, a = plot_single_category(df, category, dot_colour)
        df_by_cat = df[df.category == category]
        disk_sizes = df_by_cat.disk_size / 1024
        mem_sizes = df_by_cat['size'] / 1024
        fit_df = np.polyfit(x=disk_sizes, y=mem_sizes, deg=1)
        a.plot(disk_sizes, fit_df[1] + fit_df[0] * disk_sizes, c=line_colour,
               alpha=0.75)
        return f, a

    # plot scatter with fitted line for DataFrame objects only
    logger.info('Saving scatter_line_df_only figures (pdf, png)')
    fig_line_df_only, _ = plot_single_category_fit(df_max_filtered, 'DataFrame',
                                              'green', 'orange')
    fig_line_df_only.savefig(join(output_filepath, 'fit_line_df_only.pdf'))
    fig_line_df_only.savefig(join(output_filepath, 'fit_line_df_only.png'))

    # plot scatter with fitted line for ISA objects only
    logger.info('Saving scatter_line_isa_only figures (pdf, png)')
    fig_line_isa_only, _ = plot_single_category_fit(df_max_filtered, 'ISA', 'orange',
                                              'green')
    fig_line_isa_only.savefig(join(output_filepath, 'fit_line_isa_only.pdf'))
    fig_line_isa_only.savefig(join(output_filepath, 'fit_line_isa_only.png'))


if __name__ == '__main__':
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_fmt)

    # not used in this stub but often useful for finding various files
    project_dir = Path(__file__).resolve().parents[2]

    # find .env automagically by walking up directories until it's found, then
    # load up the .env entries as environment variables
    load_dotenv(find_dotenv())

    main()
