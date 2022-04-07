import io
import logging
import pathlib

import pandas as pd
import requests

import preprocessing

logformat = '%(levelname)s:%(module)s: %(message)s'
logging.basicConfig(level=logging.INFO, format=logformat)


def download_and_process(file_name='crashes.parquet', url=preprocessing.DATA_URL):
    """Downloads data from url and run preprocessing and store in parquet file.

    Args:
        file_name (str, optional): File name for parquet file. Defaults to 'crashes.parquet'.
        url (str, optional): URL for download. Defaults to preprocessing.DATA_URL.
    """
    logging.info(f'Downloading data from {url}')
    try:
        # download csv data with timout of 10 minutes
        response = requests.get(url=url, timeout=60*10, allow_redirects=False)
    except requests.exceptions.Timeout as e:
        logging.error(f'Timeout Exception: {e}')
    except requests.exceptions.RequestException as e:
        logging.error(f'Request Exception: {e}')
    else:
        if response.status_code != 200:
            logging.error(f'Error: status code {response.status_code}')
        else:
            logging.info('Converting csv data to pandas and parquet file')
            try:
                # convert to pandas dataframe
                csv = pd.read_csv(io.StringIO(response.content.decode('utf-8')), low_memory=False)
            except Exception as e:
                logging.error(f'Error: {e}')
            else:
                # preprocess data and write to parquet file
                preprocessing.csv_df_preprocess_to_parquet(df=csv, file_name=file_name)


def download_to_file_with_progress(file_name='crashes.csv', url=preprocessing.DATA_URL):
    """Downloads data from url and writes to file_name.

    Args:
        file_name (str, optional): File name. Defaults to 'crashes.csv'.
        url (str, optional): URL for download. Defaults to preprocessing.DATA_URL.
    """
    chunk_size = 65536  # bytes
    update_every = 10_000_000  # every 10MB
    update_divisor = int(update_every // chunk_size)  # number of chunks

    # delete old file if exists
    # pathlib.Path(file_name).unlink(missing_ok=True)  # Python 3.8+
    oldfile = pathlib.Path(file_name)  # Python 3.7-
    if oldfile.exists():
        oldfile.unlink()

    logging.info(f'Downloading data from {url}')

    with requests.get(url=url, stream=True, timeout=60*10, allow_redirects=False) as response:
        response.raise_for_status()
        with open(file_name, 'wb') as file:
            for i, chunk in enumerate(response.iter_content(chunk_size=chunk_size)):
                if chunk:  # filter out keep-alive new chunks
                    file.write(chunk)
                    if (i % update_divisor) == 0:  # about every x MB we get an update
                        logging.info(f'... downloaded {int((i * chunk_size) // 1_000_000)} MB')

    logging.info(f'Download finished, saved to "{file_name}"')


if __name__ == '__main__':
    # local testing
    download_to_file_with_progress()
    # download_and_process()
