import logging

import downloader
import preprocessing
import gitcommiter

logformat = '%(levelname)s:%(module)s: %(message)s'
logging.basicConfig(level=logging.INFO, format=logformat)

# TODO: test git commit of parquet file

def action():
    logging.info('Starting action')
    downloader.download_to_file_with_progress()
    preprocessing.read_csv_to_parquet()
    gitcommiter.commit_to_github()
    logging.info('Finished action')


if __name__ == '__main__':
    action()
