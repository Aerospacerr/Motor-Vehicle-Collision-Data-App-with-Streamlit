import logging

import downloader
import gitcommiter
import preprocessing

logformat = '%(levelname)s:%(module)s: %(message)s'
logging.basicConfig(level=logging.INFO, format=logformat)


def action():
    """Main function of the github action.
        - Downloads the csv file from the NYC website
        - Preprocesses the csv file and stores it in a parquet file
        - Uploads the parquet file to the github repository
    """
    logging.info('Starting action')
    downloader.download_to_file_with_progress()
    preprocessing.read_csv_to_parquet()
    gitcommiter.commit_to_github()
    logging.info('Finished action')


if __name__ == '__main__':
    action()
