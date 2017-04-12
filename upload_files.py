#!/user/bin/env python3
"""upload_dp_files

"""

import sys
import os
import datetime

import dropbox

# TODO: Use with, exception handling, del
#       To ensure cleanup of resources?


KILOBYTE = 1024
MEGABYTE = KILOBYTE * KILOBYTE
CHUNK_SIZE = 150 * MEGABYTE


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


def upload_file(dbx, src_base_path, dest_base_path, file_relative_path):
    src_file_path = os.path.join(src_base_path, file_relative_path)
    dest_file_path = os.path.join(dest_base_path, file_relative_path)

    if not os.path.exists(src_file_path):
        # TODO: throw exception? log? return an indicator?
        print("file '{}' does not exist!".format(src_file_path))

    file_mtimestamp = os.path.getmtime(src_file_path)
    # be sure and get time zone aware time in UTC time so that it saves with correct time
    # it is being saved who-knows-where in the world.
    file_client_modified = datetime.datetime.fromtimestamp(file_mtimestamp, datetime.timezone.utc)
    # Destinatoin TODO: Need to figure out mode and autorename parameter
    commit_info = dropbox.files.CommitInfo(path=dest_file_path, client_modified=file_client_modified)
    session_cursor = dropbox.files.UploadSessionCursor()
    with open(src_file_path, mode='rb') as file_to_upload:
        while True:
            file_meta_data = upload_next_chunk(dbx,
                                               file_to_upload,
                                               commit_info,
                                               session_cursor)
            if file_meta_data is not None:
                break
    del(session_cursor)
    return file_meta_data


if __name__ == "__main__":
    dbx = dropbox.Dropbox(intput("Enter access code:"))
    TEST_FILE_SRC_BASE_PATH ='/home/michael/Pictures/'
    TEST_FILE_DEST_BASE_PATH = '/SDK_TEST/Pictures/'
    TEST_FILE_RELATIVE_PATH = '2017/03/28/20170328_195058.jpg'
    file_meta_data = upload_file(dbx, src_base_path=TEST_FILE_SRC_BASE_PATH,
                dest_base_path=TEST_FILE_DEST_BASE_PATH,
                file_relative_path=TEST_FILE_RELATIVE_PATH)
    print("Uploaded file meta data:\n")
    print("  id={}".format(file_meta_data.id))
    print("  client_modified={}".format(file_meta_data.client_modified))
    print("  server_modified={}".format(file_meta_data.server_modified))
    print("  rev={}".format(file_meta_data.rev))
    print("  size={}".format(file_meta_data.size))
    print("  media_info={}".format(file_meta_data.media_info))
    print("  sharing_info={}".format(file_meta_data.sharing_info))
    print("  property_groups={}".format(file_meta_data.property_groups))
    print("  has_explicit_shared_members={}".format(file_meta_data.has_explicit_shared_members))
    print("  content_hash={}".format(file_meta_data.content_hash))

    # Uploaded file meta data:
    #   id=id:Wp7cobjGjOQAAAAAAAAUXw
    #   client_modified=2017-03-29 06:50:58
    #   server_modified=2017-04-12 05:45:53
    #   rev=36d30f98ee4e
    #   size=1451797
    #   media_info=None
    #   sharing_info=None
    #   property_groups=None
    #   has_explicit_shared_members=None
    #   content_hash=d21cdd9ba54ea656779c09db71cbf80938868771f8ad5acae9c55bb3cfcd4e18

    del(dbx)
