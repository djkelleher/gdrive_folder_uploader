from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
from pydrive.files import GoogleDriveFileList
import googleapiclient.errors

from pathlib import Path
import argparse
import logging
import redis
import ast
import re

logging.basicConfig(format='%(asctime)s [%(levelname)s] %(message)s',
                    level=logging.INFO)
logger = logging.getLogger("GDrive Uploader")

# Authenticate with google drive.
gauth = GoogleAuth()
# Create local webserver for authentication.
gauth.LocalWebserverAuth(
)  # client_secrets.json need to be in the same directory as this script.
drive_client = GoogleDrive(gauth)

# Connect to Redis server.
redis_client = redis.Redis(host='localhost',
                           port=6379,
                           db=0,
                           charset="utf-8",
                           decode_responses=True)


# Get files from one or more local direcotories.
def get_local_files(dirs, glob_pattern=None, re_pattern=None):
    files = []
    if not isinstance(dirs, list):
        dirs = [dirs]
    if glob_pattern is None and re_pattern is None:
        files = [f for d in dirs for f in Path(d).iterdir()]
    if glob_pattern is not None:
        files += [f for d in dirs for f in Path(d).glob(glob_pattern)]
    if re_pattern is not None:
        files += [
            f for d in dirs for f in Path(d).iterdir()
            if re.search(re_pattern, f)
        ]
    print(
        f"Found {len(files)} files in {','.join(dirs)} matching glob_pattern {glob_pattern} and re_pattern {re_pattern}."
    )
    return files


def create_gdrive_folder(dirve_client, folder_name, parent_folder_id):
    folder_obj = drive_client.CreateFile({
        'title':
        folder_name,
        'mimeType':
        'application/vnd.google-apps.folder',
        'parents': [{
            "kind": "drive#fileLink",
            "id": parent_folder_id
        }]
    })
    folder_obj.Upload()
    logger.info(
        f"Created new folder ({folder_name}) with id {folder_obj['id']}, parent id {parent_folder_id}"
    )
    return folder_obj['id']


# Create destination folder if not exists and return folder ID.
def get_folder_id(folder_path):
    folder_path = folder_path.strip("/").split("/")
    dst_folder_name = folder_path[-1]
    logger.info(f"Searching for folder '{dst_folder_name}' id")

    def search_file_tree(parent_folder_id):
        if folder_path:
            folder_name = folder_path.pop(0)
            logger.info(f"Searching for folder: {folder_name}")
            try:
                file_list = drive_client.ListFile({
                    'q':
                    f"'{parent_folder_id}' in parents and trashed=false"
                }).GetList()
            except googleapiclient.errors.HttpError as e:
                message = ast.literal_eval(e.content)['error']['message']
                if "file not found" in message.lower():
                    raise ValueError(
                        f"Folder {folder_name} was not found on Google Drive: {message}"
                    )
                else:
                    logger.error(message)
            # Look for the destination folder in the parent folder.
            for file in file_list:
                if str(file['title']) == folder_name:
                    logger.info(f"Found folder: {folder_name}")
                    if folder_name == dst_folder_name:
                        logger.info(
                            f"Found destination folder id: {file['id']}")
                        return file['id']
                    return search_file_tree(file['id'])
            # create the folder if it is not found.
            folder_id = create_gdrive_folder(drive_client, folder_name,
                                             parent_folder_id)
            if folder_name == dst_folder_name:
                logger.info(f"Found destination folder id: {folder_id}")
                return folder_id
            return search_file_tree(folder_id)

    return search_file_tree('root')


def upload_file(local_file, dst_folder_id):
    f = drive_client.CreateFile({
        'title':
        Path(local_file).name,
        "parents": [{
            "kind": "drive#fileLink",
            "id": dst_folder_id
        }]
    })
    f.SetContentFile(str(local_file))
    f.Upload()


def upload_files(dst_dir, overwrite_existing=False, key='gdrive_upload'):
    dst_dir_id = get_folder_id(dst_dir)
    logger.info(
        f"Uploading files to Google Drive folder '{dst_dir}' (id {dst_dir_id})"
    )
    count = 0
    while True:
        local_file = redis_client.spop(key)
        if local_file is None:
            logger.info("All files are uploaded!")
            return
        logger.info(f"Uploading file {local_file}")
        upload_file(local_file, dst_dir_id)
        count += 1
        if count % 50 == 0:
            remaining = redis_client.scard(key)
            logger.info(f"Remaining file count: {remaining}")


def cache_upload_files(local_files,
                       dst_dir,
                       overwrite_existing=False,
                       key='gdrive_upload'):
    if not overwrite_existing:
        dst_folder_id = get_folder_id(dst_dir)
        existing_files = drive_client.ListFile({
            'q':
            f"'{dst_folder_id}' in parents and trashed=false"
        }).GetList()
        logger.info(
            f"Found {len(existing_files)} existing files in destination folder {dst_dir}."
        )
        local_files = set(local_files).difference(existing_files)
    logger.info(f"Caching {len(local_files)} local files for upload.")
    for file in local_files:
        redis_client.sadd(key, str(file))
    logger.info(f"Finished caching local files.")


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-s',
        '--src',
        type=str,
        help='Path to directory containing files to cache for upload.')
        # Files from src are cached in a Redis set. Multiple script instances can simultaneously work off the same set.
        # If running multiple script instances, only pass src argument when the first script is started.
    parser.add_argument(
        '-d',
        '--dst',
        type=str,
        required=True,
        help='Google Drive directory where files will be uploaded to.')
    parser.add_argument('-g',
                        '--glob',
                        type=str,
                        default=None,
                        help='Glob pattern for matching file names.')
    parser.add_argument('-r',
                        '--re',
                        type=str,
                        default=None,
                        help="Regex pattern for matching file names.")
    parser.add_argument(
        '-o',
        '--overwrite_existing',
        action='store_true',
        help='Overwrite existing files if file already exists in Google Drive.'
    )
    return parser.parse_args()


def main():
    args = parse_args()
    if args.src:
        files = get_local_files(dirs=args.src,
                                glob_pattern=args.glob,
                                re_pattern=args.re)
        cache_upload_files(local_files=files,
                           dst_dir=args.dst,
                           overwrite_existing=args.overwrite_existing)
    upload_files(dst_dir=args.dst, overwrite_existing=args.overwrite_existing)


if __name__ == "__main__":
    main()
