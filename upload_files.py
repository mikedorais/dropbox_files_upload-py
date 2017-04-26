#!/user/bin/env python3
"""upload_dp_files

"""

import sys
import os
import time
import datetime
from pathlib import Path

import dropbox

# TODO: Use with, exception handling, del
#       To ensure cleanup of resources?


KILOBYTE = 1024
MEGABYTE = KILOBYTE * KILOBYTE
CHUNK_SIZE = 20 * MEGABYTE  # Dropbox documented max is 150 MB


def upload_next_chunk(dbx, file_to_upload, commit_info, session_cursor):
    """ Upload the first, next and, last chunk of a file to Dropbox account

    Args:
        dbx (dropbox.Dropbox): Initialized Dropbox account
        file_to_upload (stream): Opened stream to upload, or being uploaded
        commit_info (dropbox.files.CommitInfo): Information for how file should be committed to account
        sesssion_cursor (dropbox.files.UploadSessionCursor): Session cursor
            at beginning of stream writing, intialized to UploadSessionCursor()
            first call will populate it appropriately for subsequent calls.

    Returns: None if there is more to read and upload,
             dropbox.files.FileMetadata when the upload is complete
    """
    file_chunk = file_to_upload.read(CHUNK_SIZE)
    file_chunk_len = len(file_chunk)
    if not hasattr(session_cursor, 'session_id'):
        # First chunk read.  Either upload if less then CHUNK_SIZE or start session
        if file_chunk_len < CHUNK_SIZE:
            file_meta_data = dbx.files_upload(file_chunk,
                                              commit_info.path,
                                              client_modified=commit_info.client_modified)
            # TODO: Handle exceptions.  Assert that file_meta_data is not be null.
        else:
            # TODO: Check for success
            session_start_result = dbx.files_upload_session_start(file_chunk)
            session_cursor.session_id = session_start_result.session_id
            file_meta_data = None
    elif file_chunk_len < CHUNK_SIZE:
        # TODO: Check for success
        file_meta_data = dbx.files_upload_session_finish(file_chunk,
                                                         session_cursor,
                                                         commit_info)
        # TODO: Handle exceptions.  Assert that file_meta_data is not be null.
    else:
        # TODO: Check for success
        dbx.files_upload_session_append_v2(file_chunk, session_cursor)
        file_meta_data = None
        # TODO: Handle exceptions.
    session_cursor.offset = file_to_upload.tell()

    if (file_meta_data is not None
            and file_meta_data.size != session_cursor.offset):
        print("uploaded size {} does not equal file size{}".format(
            file_meta_data.size, session_cursor.offset))
    return file_meta_data


def upload(dbx, src_path, dest_path, log_file_stream):
    if not src_path.exists():
        # TODO: throw exception? log? return an indicator?
        print("'{}' does not exist!".format(str(src_path)))
    if src_path.is_file():
        # TODO: Is there a better way to skip disallowed names?
        #       On Thumes.db get dropbox.exceptions.ApiError: ApiError('2a0f7718b501fc86e0ce9ef462733970', UploadError('path', UploadWriteFailed(reason=WriteError('disallowed_name', None),
        if src_path.name.upper() == "THUMBS.DB":
            return
        src_path_string = str(src_path)
        dest_path_string = str(dest_path)
        file_mtimestamp = os.path.getmtime(src_path_string)
        # be sure and get time zone aware time in UTC time so that it saves with correct time
        # it is being saved who-knows-where in the world.
        file_client_modified = datetime.datetime.fromtimestamp(file_mtimestamp, datetime.timezone.utc)
        # Destinatoin TODO: Need to figure out mode and autorename parameter
        commit_info = dropbox.files.CommitInfo(path=dest_path_string, client_modified=file_client_modified)
        session_cursor = dropbox.files.UploadSessionCursor()
        print("Copying file: '{}'\n".format(src_path_string))
        with open(src_path_string, mode='rb') as file_to_upload:
            while True:
                file_meta_data = upload_next_chunk(dbx,
                                                file_to_upload,
                                                commit_info,
                                                session_cursor)
                if file_meta_data is not None:
                    break
                time.sleep(.1)
        del(session_cursor)
        print("Copied file: '{}'\n".format(src_path_string))
        log_file_stream.write(src_path_string)
        log_file_stream.write(",")
        log_file_stream.write(str(file_client_modified))
        log_file_stream.write(",")
        log_file_stream.write(dest_path_string)
        # print("Uploaded file meta data:\n")
        log_file_stream.write(",")
        log_file_stream.write(file_meta_data.id)
        log_file_stream.write(",")
        log_file_stream.write(str(file_meta_data.client_modified))
        log_file_stream.write(",")
        log_file_stream.write(str(file_meta_data.server_modified))
        log_file_stream.write(",")
        log_file_stream.write(file_meta_data.rev)
        log_file_stream.write(",")
        log_file_stream.write(str(file_meta_data.size))
        log_file_stream.write(",")
        log_file_stream.write(file_meta_data.content_hash)
        log_file_stream.write("\n")
        # print("  media_info={}".format(file_meta_data.media_info))
        # print("  sharing_info={}".format(file_meta_data.sharing_info))
        # print("  property_groups={}".format(file_meta_data.property_groups))
        # print("  has_explicit_shared_members={}".format(file_meta_data.has_explicit_shared_members))
        # print("  content_hash={}".format(file_meta_data.content_hash))
    elif not src_path.is_dir():
        pass
    else:
        for item in src_path.iterdir():
            upload(dbx, item, dest_path.joinpath(item.name), log_file_stream)


def main(dbx, src_base_path, dest_base_path, target_relative_path, log_file_path):
    src_path_string = os.path.join(src_base_path, target_relative_path)
    dest_path_string = os.path.join(dest_base_path, target_relative_path)
    src_path = Path(src_path_string)
    dest_path = Path(dest_path_string)
    log_file_stream = open(log_file_path, 'wt')
    try:
        upload(dbx, src_path, dest_path, log_file_stream)
    finally:
        log_file_stream.close()
        del(log_file_stream)


if __name__ == "__main__":
    if len(sys.argv) == 5:
        SRC_BASE_PATH = sys.argv[1].strip()
        DEST_BASE_PATH = sys.argv[2].strip()
        FILE_RELATIVE_PATH = sys.argv[3].strip()
        LOG_FILE_PATH = sys.argv[4].strip()
    else:
        SRC_BASE_PATH = '/home/michael/Documents/'
        DEST_BASE_PATH = '/SDK_TEST/'
        FILE_RELATIVE_PATH = 'My Vocation'
        LOG_FILE_PATH = "/home/michael/Documents/My Vocation.dbxup.log"

    access_code = input("Enter access code:")
    print("\n{}\n".format(access_code))
    dbx = dropbox.Dropbox(access_code, timeout=60)

    main(dbx, src_base_path=SRC_BASE_PATH,
                dest_base_path=DEST_BASE_PATH,
                target_relative_path=FILE_RELATIVE_PATH,
                log_file_path=LOG_FILE_PATH)
    del(dbx)

# def listdir(src_path, dest_path):
#     if not src_path.exists():
#         # TODO: throw exception? log? return an indicator?
#         print("'{}' does not exist!".format(str(src_path)))
#     if src_path.is_dir():
#         for item in src_path.iterdir():
#             listdir(item, dest_path.joinpath(item.name))
#     elif src_path.is_file():
#         print(str(src_path))
#         print(str(dest_path))