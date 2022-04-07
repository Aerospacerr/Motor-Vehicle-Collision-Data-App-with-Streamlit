import logging
import os
import pathlib

from github import Github

# TODO: refactor this file

logging.basicConfig(level=logging.INFO)

# read github token from os env
GITHUB_TOKEN = os.environ.get('GITHUB_TOKEN')
GITHUB_REPOSITORY = os.environ.get('GITHUB_REPOSITORY')

# check if token and repo is set
assert GITHUB_TOKEN, "GITHUB_TOKEN could not be loaded from env"
assert GITHUB_REPOSITORY, "GITHUB_REPOSITORY could not be loaded from env"

logging.info(f'Github repository: {GITHUB_REPOSITORY}')

# get github repo object from remote repo
g = Github(GITHUB_TOKEN)
repo = g.get_repo(GITHUB_REPOSITORY)

parquet_file_name = "crashes.parquet"
parquet_file_exists = False

try:
    # TODO: check if binary read is possible
    parquet_file_contents = repo.get_contents(parquet_file_name)
    # decoded_contents = ini_file_contents.decoded_content.decode("utf-8")
except Exception as e:
    logging.error(e)
    logging.error(
        f'Could not find file "{parquet_file_name}" in the repository')
    ini_file_exists = False
else:
    ini_file_exists = True

# read binary content of parquet file
content = pathlib.Path(parquet_file_name).read_bytes()

if parquet_file_exists:
    # TODO: check if binary write of content is possible
    repo.update_file(path=parquet_file_name,
                    message=f'Auto Update of {parquet_file_name}', content=content, sha=parquet_file_contents.sha)
else:
    # TODO: check if binary write of content is possible
    # create file if it not exists in repo
    repo.create_file(path=parquet_file_name,
                    message=f'Auto Creation of {parquet_file_name}', content=content)
