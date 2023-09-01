"""s3 utilities"""
import json
import yaml
import boto3
from botocore.exceptions import ClientError
from util import log


@log.logs
def list_s3_files(bucket: str, folder_path: str) -> list:
    """
    lists all files inside a bucket or folder from s3
    :param bucket: s3 bucket name
    :param folder_path: caminho da pasta até o local desejado
    :return: lista com todos os nomes de arquivos
    """
    fsmg = f'{__name__}:{list_s3_files.__name__}'
    files_found = None
    try:
        s3 = boto3.resource('s3')
        bucket_name = s3.Bucket(bucket)
        files_found = [obj for obj in bucket_name.objects.filter(Prefix=folder_path) if
                       obj.key[-1] != '/']
    except Exception as e:
        log.createLogger(fsmg).error(f'Erro ao obter arquivos do S3: {e}')
    return files_found


@log.logs
def move_file_s3(source_bucket_name: str, destination_bucket_name: str, file_key: str, new_file_key: str):
    """
    Copies a file to another folder in S3 and deletes the old one
    :param source_bucket_name: bucket of origin
    :param destination_bucket_name: new bucket name
    :param file_key: the file key
    :param new_file_key: the new file key
    """
    fsmg = f'{__name__}:{move_file_s3.__name__}'

    try:
        s3 = boto3.client('s3')
        s3.copy_object(
            Bucket=destination_bucket_name,
            CopySource={'Bucket': source_bucket_name, 'Key': file_key},
            Key=new_file_key
        )
        s3.delete_object(Bucket=source_bucket_name, Key=file_key)
    except Exception as e:
        log.createLogger(fsmg).error(f'Error while copying or deleting S3 file: {e}')
        raise ValueError("s3 error")


@log.logs
def read_json_from_s3_object(bucket: str, key: str) -> dict:
    """
    Read JSON file in s3 bucket
    :param bucket: s3 bucket name
    :param key: filename
    :return: JSON file as a python dictionary
    """
    fsmg = f'{__name__}:{read_json_from_s3_object.__name__}'
    try:
        s3_client = boto3.client('s3')
        response = s3_client.get_object(Bucket=bucket, Key=key)
        json_object = response['Body'].read().decode('utf-8')
    except Exception as err:
        log.createLogger(fsmg).error(f'Error while reading Bucket={bucket}, Key={key}: {err}')
        raise Exception(f'Error while reading Bucket={bucket}, Key={key}: {err}')
    return json.loads(json_object)


@log.logs
def read_yaml_from_s3_object(bucket: str, key: str) -> dict:
    """
    Read YAML file in s3 bucket
    :param bucket: s3 bucket name
    :param key: filename
    :return: YAML file as a python dictionary
    """
    fsmg = f'{__name__}:{read_json_from_s3_object.__name__}'
    try:
        s3_client = boto3.client('s3')
        response = s3_client.get_object(Bucket=bucket, Key=key)
        yaml_object = response['Body'].read()
    except Exception as err:
        log.createLogger(fsmg).error(f'Error while reading Bucket={bucket}, Key={key}: {err}')
        raise Exception(f'Error while reading Bucket={bucket}, Key={key}: {err}')

    return yaml.safe_load(yaml_object)


@log.logs
def read_file_from_s3_object(bucket: str, key: str) -> bytes:
    """
    Read file in s3 bucket
    :param bucket: s3 bucket name
    :param key: filename
    :return: file bytes
    """
    fsmg = f'{__name__}:{read_file_from_s3_object.__name__}'
    response = None
    try:
        s3_client = boto3.client('s3')
        response = s3_client.get_object(Bucket=bucket, Key=key)
    except Exception as err:
        log.createLogger(fsmg).error(f'Error while readingS3 file: {err}')

    return response['Body'].read()


@log.logs
def save_file_to_s3_bucket2(file_path: str, bucket: str, key: str):
    """
    save binary file to s3 bucket
    :param file_path: file path
    :param bucket: s3 bucket name
    :param key: filename
    :return: dictionary with response
    """
    s3_client = boto3.resource('s3')

    fsmg = f'{__name__}:{save_file_to_s3_bucket2.__name__}'
    logger = log.createLogger(fsmg)
    try:
        response = s3_client.meta.client.upload_file(file_path, bucket, key)

    except ClientError as exc:
        logger.error(f'Wrong key or bucket: file_path={file_path}, bucket={bucket}, key={key}')
        raise ValueError(f'Wrong key or bucket: file_path={file_path}, bucket={bucket}, key={key}') from exc
    else:
        logger.info(f'*** Added report to bucket: {bucket}, with key: {key}')
        return response
