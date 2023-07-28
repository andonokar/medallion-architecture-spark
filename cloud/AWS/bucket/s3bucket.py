import datetime
import os
from typing import List, Dict
import boto3


class Bucket():

    def __init__(self, parameters: Dict) -> None:
        """
        parameters =  {
            "bucket_name_src": "domrock-afinz-dev", bucket de origem
            "bucket_name_dst": "domrock-afinz-dev",  Bucket de destino
            "src_dir": "sftp/CORREIOS",  local no s3 onde o aos arquivos se encontam
            "dest_dir": "/tmp/",   diretorio de destinno
            "dir_dest": "/tmp/sftp/CORREIOS/", local onde o arquivo sera enviado na maquina local
            "copy_bucket_dir": "processed/CORREIOS/"  local para onde o arquivo será enviado apos a finalizacao do processo
          }
        :param parameters: It is a dict which contains a set of input data and configure the operation environment.

        Exampla of use this class

        if __name__ == "__main__":
            parameters =  {
            "bucket_name_src": "domrock-afinz-dev", bucket de origem
            "bucket_name_dst": "domrock-afinz-dev",  Bucket de destino
            "src_dir": "sftp/CORREIOS",  local no s3 onde o aos arquivos se encontam
            "dest_dir": "/tmp/",   diretorio de destinno
            "dir_dest": "/tmp/sftp/CORREIOS/", local onde o arquivo sera enviado na maquina local
            "copy_bucket_dir": "processed/CORREIOS/"  local para onde o arquivo será enviado apos a finalizacao do processo
          }
        b = Bucket(parameters)
        b.download_object
        b._copy_data()
        b._delete_data()
        """
        self._client = boto3.client("s3")
        # self._resource = boto3.resource("s3")
        self._bucket_name_src = parameters['bucket_name_src']
        self._bucket_name_dst = parameters['bucket_name_dst']
        self._src_dir = parameters['src_dir']
        self._dest_dir = parameters['dest_dir']
        self._bucket_dir = parameters['copy_bucket_dir']
        self._get_list_of_object = self._get_list_of_files()

    def _get_list_of_files(self) -> List:
        """
        This method return a list of objects sotored on a specific folder on S3 bucket.
        :return:
        """
        list_of_object = []
        object_s3 = self._client.list_objects_v2(Bucket=self._bucket_name_src, Prefix=self._src_dir)

        for content in object_s3['Contents']:
            list_of_object.append(content['Key'])
        return list_of_object

    def copy_data(self) -> str:
        """
        This method copies copy set of data from a folder on a s3 bucket to another folder on bucket.
        :return:

        """
        result: str = "OK"
        now = datetime.datetime.now()

        for filename in self._get_list_of_object:
            try:
                filename_dest = filename.replace(self._src_dir, self._bucket_dir + now.isoformat())
                copy_source = {
                    'Bucket': self._bucket_name_src,
                    'Key': filename
                }

                self._client.copy(CopySource=copy_source, Bucket=self._bucket_name_dst, Key=filename_dest)
            except Exception as err:
                result = str(err)
        return result

    def delete_data(self) -> str:
        """
            This method is responsible to delete a set of files on a  s3 bucket.
        :return: a OK string if the operation was successful or error message otherwise.
        """
        result = "OK"
        list_of_objects = self._get_list_of_object
        object_to_delete = []
        if len(list_of_objects) > 1:
            list_of_objects.pop(0)
            try:
                for key in list_of_objects:
                    object_to_delete.append({"Key": key})
                self._client.delete_objects(Bucket=self._bucket_name_src, Delete={"Objects": object_to_delete})
            except Exception as err:
                result = str(err)

        return result

    def download_object(self) -> str:
        """
            This method downloads a set of file to a local machine or ccontainer.
        :return: string Ok or Error value if the operation wasn't successful
        """
        result: str = "OK"
        for filename in self._get_list_of_object:
            try:
                head, tail = os.path.split(filename)
                if tail != '':
                    # print(self._dest_dir + head + '/' + tail, filename)
                    self._client.download_file(self._bucket_name_src, filename, self._dest_dir + head + '/' + tail)
                else:
                    # print('criando o objeto', self._dest_dir + head)
                    if not (os.path.exists(self._dest_dir + head)):
                        os.makedirs(self._dest_dir + head)

            except self._client.exception as err:
                result = str(err)
        return result



    """
    if __name__ == "__main__":
        parameters = {
            'bucket_name': 'domrock-prdaffix-dev',
            'src_dir': 'sftp',
            'dest_dir': '/tmp/',
            'copy_bucket_dir': 'processed'
        }
        b = Bucket(parameters)
        b.download_object
        b.copy_data()
        b.delete_data()
    
    """
