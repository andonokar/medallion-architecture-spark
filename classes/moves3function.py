import boto3
from datetime import datetime
from util import log


@log.logs
def prepare_moving_folder(source_bucket, folder, movefolder):
    """
    Moves all files in the source folder of the source bucket to the destination folder of the destination bucket,
    but first create a subfolder with the datetime for all the moved files
    :param source_bucket: the bucket where the files are from
    :param folder: the folder where the files are from
    :param movefolder: configuration for the folder names
    """
    now = datetime.now()
    date_str = now.strftime('%Y-%m-%d')
    time_str = now.strftime('%H-%M')
    show = f"{date_str}-{time_str}"
    try:
        move_s3_folder(source_bucket, folder,
                       movefolder["destinationbucket"],
                       f"{movefolder['firstfoldername']}{folder}{show}/")
    except KeyError as err:
        raise KeyError(f'erro na configuracao movefolder: {err} nao existe')


def move_s3_folder(source_bucket, source_prefix, destination_bucket, destination_prefix):
    """
    Moves all files in the source folder of the source bucket to the destination folder of the destination bucket.
    :param source_bucket: the bucket where the files are from
    :param source_prefix: the folder the files are from
    :param destination_bucket: the bucket where the files will be moved
    :param destination_prefix: the folder where the files will be moved
    """
    s3 = boto3.resource('s3')

    # Get the source bucket and folder
    source_bucket = s3.Bucket(source_bucket)
    source_objects = source_bucket.objects.filter(Prefix=source_prefix)

    # Move each object to the destination folder
    for obj in source_objects:
        source_key = obj.key
        destination_key = destination_prefix + source_key[len(source_prefix):]
        s3.Object(destination_bucket, destination_key).copy_from(
            CopySource={'Bucket': source_bucket.name, 'Key': source_key}
        )
        obj.delete()
