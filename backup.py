#!/usr/bin/env python
import os, sys
from datetime import date
import time
from boto.s3.connection import S3Connection
from boto.s3.key import Key
from boto.exception import S3CreateError
import tarfile
from subprocess import Popen, PIPE
import StringIO

#SETTINGS
from backup_settings import *

def get_user_media(back_up_dir=None):
    """
    For a django project, return a path to a tar.gz file that
    represents all user media on the server.
    
    We user tar to create a file on the server and then
    push that file to s3. We then delete the local file, once the
    file has been pushed. We don't 
    """
    source = settings.MEDIA_ROOT
    destination = back_up_dir
    target_backup = destination + time.strftime('%Y%m%d%H%M%S') + 'tar.gz'    
    tar = tarfile.open(target_backup, "w:gz")
    tar.add(source)
    tar.close()
    return target_backup


# Returns the value of a django manage.py dumpdata command as a string.
def get_dumped_data(*apps, **options):
    from django.core.management import call_command
    output = StringIO.StringIO()

    # Redirect stdout to output var
    
    sys.stdout = output    
    call_command('dumpdata')
    sys.stdout = sys.__stdout__
    
    o = output.getvalue()
    output.close()
    o.rstrip()
    return o

# Returns output of a postgres dump as a string

def get_postgres_dump(dbname):
    process = Popen(["pg_dump", dbname], stdout=PIPE)
    output = process.communicate()[0]
    return output
    
# Returns a connection, bucket, and/or key for an s3 account
# check out usage below
def s3_init(access_key_id, secret_key, bucket_name=None, key_name=None):
    conn = S3Connection(access_key_id, secret_key)    
    if bucket_name is not None:
        # Try and create the bucket in case it doesn't exist
        try:
            bucket = conn.create_bucket(bucket_name)
        except S3CreateError:
            bucket = conn.get_bucket(bucket_name)
        if key_name is not None:
            key = Key(bucket)
            key.key = key_name
            return conn, bucket, key
        else:
            return conn, bucket
    else:
        return conn, bucket, key

def send_to_s3(items=None, is_binary=False):
    """
    For items in an iterable, send them to your s3 account
    """
    conn, bucket = s3_init(AWS_ACCESS_KEY_ID, AWS_SECRET_KEY, BUCKET_NAME)
    for label, data in items:
        key = Key(bucket)
        key.key = label
        for item in bucket.list():
            local_md5 = hashlib.md5(data).hexdigest()
            if item.name == label: 
                key.open()
                key.close(); #loads key.etag
                # remote hash
                remote_md5 = key.etag.replace('\"','') # clears quote marks
                # If new backup is different than the last saved one, update it
                if local_md5 != remote_md5:
                    if is_binary:
                        key.set_contents_from_filename(data)
                    else:
                        key.set_contents_from_string(data)
        else:
            if is_binary:
                key.set_contents_from_filename(data)
            else:
                key.set_contents_from_string(data)



if __name__ == '__main__':
    #init pypath and environ vars
    sys.path.extend([PROJECT_DIR, PROJECT_DIR+'/..'])
    sys.path.insert(0, PROJECT_DIR+'/externals')
    sys.path.insert(0, PROJECT_DIR+'/apps')
    os.environ['DJANGO_SETTINGS_MODULE'] = SETTINGS_PYPATH
    from django.conf import settings

    # Get the dumps as strings
    print("Dumping data.")
    dumped_data = get_dumped_data()
    #pg_dump = get_postgres_dump(settings.DATABASE_NAME)
    # Get binary data
    user_media = get_user_media(BACKUP_DIR)


    #These are the strings (and their key names) that will get backed up.
    # To back up more stuff, just add to this dict.
    # NOTE: Our s3 code sniffs if the datatype is binary.
    #'dumpdata': dumped_data,
    # 'pg_dump': pg_dump
    # 'user_media' : user_media
    text_data = {'dumpdata': dumped_data,}
    bin_data = {'user_media': user_media, }

    # Initialize S3 connection
    

    import hashlib
    
    # For each thing to back up, back it up
    print("Connecting to AWS.")
    send_to_s3(items=text_data.items(), is_binary=False)
    send_to_s3(items=bin_data.items(), is_binary=True)
    
    

            
