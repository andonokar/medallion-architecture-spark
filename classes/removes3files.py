from util import log
import boto3
from datetime import datetime, timezone, timedelta


@log.logs
def remove_old_files(bucket_name, hours):
    fmsg = f'{remove_old_files.__name__}'
    logger = log.createLogger(fmsg)
    # Create a connection to the S3 service
    s3 = boto3.resource('s3')
    # Get the S3 bucket object
    bucket = s3.Bucket(bucket_name)
    # Calculate the date threshold (one month ago)
    threshold = datetime.now(timezone.utc) - timedelta(hours=hours)
    objects = [obj for obj in bucket.objects.filter(Prefix="") if
               obj.key[-1] != '/' and obj.last_modified < threshold]
    for obj in objects:
        obj.delete()
        logger.info(f"Deleted file: {obj.key}")
