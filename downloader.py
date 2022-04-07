import io
import logging

import pandas as pd
import requests

import preprocessing
# TODO: add git commit of parquet file
# import gitcommiter

logging.basicConfig(level=logging.INFO)

# TODO: refactor this file to use requests in streaming mode?
# TODO: refactor this file to download file first?

def main():
    logging.info(f'Downloading data from {preprocessing.DATA_URL}')
    try:
        # download csv data with timout of 15 minutes
        response = requests.get(url=preprocessing.DATA_URL, timeout=60*15, allow_redirects=False)
    except requests.exceptions.Timeout as e:
        logging.error(f'Timeout error: {e}')
    except requests.exceptions.RequestException as e:
        logging.error(e)
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
                preprocessing.csv_df_preprocess_to_parquet(df=csv, file_name='crashes.parquet')


if __name__ == '__main__':
    main()


# https://stackoverflow.com/questions/37573483/progress-bar-while-download-file-over-http-with-requests
# https://stackoverflow.com/questions/56795227/how-do-i-make-progress-bar-while-downloading-file-in-python

# version with progress bar:
# from tqdm import *
# import requests
# url = preprocessing.DATA_URL
# name = "crashes.csv"
# with requests.get(url, stream=True) as r:
#     r.raise_for_status()
#     with open(name, 'wb') as f:
#         pbar = tqdm(total=int(r.headers['Content-Length']))
#         for chunk in r.iter_content(chunk_size=8192):
#             if chunk:  # filter out keep-alive new chunks
#                 f.write(chunk)
#                 pbar.update(len(chunk))
