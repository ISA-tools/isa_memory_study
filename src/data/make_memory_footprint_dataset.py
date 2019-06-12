# -*- coding: utf-8 -*-
import click
import logging
from pathlib import Path
from dotenv import find_dotenv, load_dotenv
from isatools.isatab import load
from glob import glob
from os.path import join, exists, getsize
from os import mkdir
from sys import getsizeof, stderr, exit
from itertools import chain
from collections import deque
try:
    from reprlib import repr
except ImportError:
    pass
import pandas as pd
from tqdm import tqdm


# based on http://code.activestate.com/recipes/577504/
def total_size(o, handlers={}, verbose=False):
    """ Returns the approximate memory footprint an object and all of its contents.

    Automatically finds the contents of the following builtin containers and
    their subclasses:  tuple, list, deque, dict, set and frozenset.
    To search other containers, add handlers to iterate over their contents:

        handlers = {SomeContainerClass: iter,
                    OtherContainerClass: OtherContainerClass.get_elements}

    """
    logger = logging.getLogger(__name__)
    all_handlers = {
        tuple: iter,
        list: iter,
        deque: iter,
        dict: lambda d: chain.from_iterable(d.items()),
        set: iter,
        frozenset: iter,
    }
    all_handlers.update(handlers)
    seen = set()
    default_size = getsizeof(0)

    def sizeof(o):
        if id(o) in seen:       # do not double count the same object
            return 0
        seen.add(id(o))
        s = getsizeof(o, default_size)

        if verbose:
            logger.info(s, type(o), repr(o), file=stderr)

        for typ, handler in all_handlers.items():
            if isinstance(o, typ):
                s += sum(map(sizeof, handler(o)))
                break
        return s

    return sizeof(o)


@click.command()
@click.argument('input_filepath', type=click.Path(exists=True))
@click.argument('output_filepath', type=click.Path(exists=False))
def main(input_filepath, output_filepath):
    """ Runs data processing scripts to run measurements on the metadata found
        in data/interim to extract the loaded object sizes of the ISA objects,
        Pandas DataFrame objects, and the raw size of the files as reported on
        disk.

        We iterate through our processed data MTBLS metadata and run ISA-Tab
        load on each, and then extract the approximate size of the DAG portion
        of the metadata. This DAG metadata is analogous to each line of the
        table files that describes one path in the DAGs.
    """
    print("IN", input_filepath)
    print("TARGET OUT:",output_filepath)

    logger = logging.getLogger(__name__)
    logger.info('making final data set from processed data')
    if exists(output_filepath):
        if getsize(output_filepath) > 0:
            logger.info('Output file {} already contains data. '
                        'Skipping writing to data/processed. If this is not '
                        'expected, do you need to "make clean" first?'.format(
                         output_filepath))
            exit(0) 
    # else:
    else:
        try:
            mkdir('./data/processed')
        except Error as e:
            logging.error(e)

    with open(output_filepath, 'w') as output_file:
            output_file.write('fname,disk_size,df_size,isa_size,mtblsid\n')
            logger.info('fname', ['disk_size', 'df_size', 'isa_size'],'mtblsid',)
            for study_dir in tqdm(glob(join(input_filepath, 'MTBLS*'))):
                try:
                    print(study_dir)
                    isa = load(study_dir)
                    for s in isa.studies:
                        if 'MTBLS' in s.identifier:
                            mtblsid = s.identifier
                        else:
                            print(study_dir)
                            mtblsid = study_dir

                        fname = s.filename
                        df = pd.read_csv(join(study_dir, fname), sep='\t')
                        df_size = total_size(df, verbose=False)
                        disk_size = getsize(join(study_dir, fname))
                        isa_size = total_size(s.process_sequence, verbose=False)
                        output_file.write('{},{},{},{},{}\n'.format(
                                         fname, disk_size, df_size, isa_size, mtblsid+"_s"))
                        for a in s.assays:
                            fname = a.filename
                            df = pd.read_csv(join(study_dir, fname), sep='\t')
                            df_size = total_size(df, verbose=False)
                            disk_size = getsize(join(study_dir, fname))
                            isa_size = total_size(a.process_sequence, verbose=False)
                            output_file.write('{},{},{},{},{}\n'.format(
                                              fname, disk_size, df_size, isa_size,mtblsid+"_a"))
                    output_file.flush()
                except KeyboardInterrupt:
                    exit(1)


if __name__ == '__main__':
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_fmt)

    # not used in this stub but often useful for finding various files
    project_dir = Path(__file__).resolve().parents[2]

    # find .env automagically by walking up directories until it's found, then
    # load up the .env entries as environment variables
    load_dotenv(find_dotenv())

    main()
