import logging
import os
import pathlib

from github import Github, GithubException

logformat = '%(levelname)s:%(module)s: %(message)s'
logging.basicConfig(level=logging.INFO, format=logformat)


def commit_to_github(file_name='crashes.parquet'):
    # read github token from os env
    GITHUB_TOKEN = os.environ.get('GITHUB_TOKEN')
    GITHUB_REPOSITORY = os.environ.get('GITHUB_REPOSITORY')
    GITHUB_REF = os.environ.get('GITHUB_REF')

    # check if env variables are set
    assert GITHUB_TOKEN, "GITHUB_TOKEN could not be loaded from env"
    assert GITHUB_REPOSITORY, "GITHUB_REPOSITORY could not be loaded from env"
    assert GITHUB_REF, "GITHUB_REF could not be loaded from env"

    # get github repo object from remote repo
    github_ = Github(GITHUB_TOKEN)
    repo = github_.get_repo(GITHUB_REPOSITORY)

    logging.info(f'Github repository: {GITHUB_REPOSITORY}')
    logging.info(f'Github branch: {GITHUB_REF}')

    parquet_file_name = file_name
    parquet_file_exists = False

    try:
        logging.info(f'Checking if file "{parquet_file_name}" exists')
        parquet_file_contents = repo.get_contents(path=parquet_file_name, ref=GITHUB_REF)  # ok, we have the content
    except GithubException:
        # if file is larger than 1 MB than this workaround is needed
        logging.info('Checking for large file > 1MB')
        parquet_file_contents = get_blob_content(repo=repo, branch=GITHUB_REF, path_name=parquet_file_name)
        parquet_file_exists = True
    except Exception as e:
        logging.error(e)
        logging.error(f'Could not find file "{parquet_file_name}" in the repository')
        parquet_file_exists = False
    else:
        parquet_file_exists = True

    logging.info(f'Commiting file "{parquet_file_name}" content')

    # read binary content of parquet file
    content = pathlib.Path(parquet_file_name).read_bytes()

    if parquet_file_exists:
        repo.update_file(path=parquet_file_name,
            message=f'Auto Update of {parquet_file_name}', content=content, sha=parquet_file_contents.sha)
    else:
        # create file if it not exists in repo
        repo.create_file(path=parquet_file_name,
            message=f'Auto Creation of {parquet_file_name}', content=content)

    logging.info(f'File "{parquet_file_name}" content commited')


def get_blob_content(repo, branch, path_name):
    """This is a helper function to get the content of a large file.

    Args:
        repo (_type_): _description_
        branch (_type_): _description_
        path_name (_type_): _description_

    Returns:
        _type_: _description_
    """
    # first get the branch reference
    ref = repo.get_git_ref(f'heads/{branch}')
    # then get the tree
    tree = repo.get_git_tree(ref.object.sha, recursive='/' in path_name).tree
    # look for path in tree
    sha = [x.sha for x in tree if x.path == path_name]
    if not sha:
        # well, not found..
        return None
    # we have sha
    return repo.get_git_blob(sha[0])


if __name__ == '__main__':
    # local testing:
    from dotenv import load_dotenv
    load_dotenv()
    commit_to_github()
