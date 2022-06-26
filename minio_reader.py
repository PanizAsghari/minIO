import os
import boto3
from botocore.client import Config
import os
###this part takes care of the minio bucket of our interest. it copies the data in our server to be processed by next step
### and deletes it from minio if wanted
class MinioReader():
    def __init__(self,information):
        self.url = os.environ["MINIOURL"]
        self.key = os.environ["MINIOUSER"]
        self.password = os.environ["MINIOPASS"]
        self.s3 = boto3.resource('s3',
                            endpoint_url=self.url,
                            aws_access_key_id=self.key,
                            aws_secret_access_key=self.password,
                            config=Config(signature_version='s3v4'),
                            region_name='us-east-1')

        self.srcbucket = self.s3.Bucket(self.bucket_name)
        self.destination_path=(information)
        self.delete=(information)
        self.extension=(information)
        self.create_directory()

    @property
    def destination_path(self):
        return self._destination_path

    @destination_path.setter
    def destination_path(self,information):
        self._destination_path=information['local_path']

    @property
    def bucket_name(self):
        return self._bucket_name

    @bucket_name.setter
    def bucket_name(self,information):
        self._bucket_name= information['minio_bucket']

    @property
    def delete(self):
        return self._delete
        
    @delete.setter
    def delete(self,information):
        if 'delete_from_minio' in information:
            self._delete= information['delete_from_minio']
        else:
            self._delete= 1

    @property
    def extension(self):
        return self._extension

    @extension.setter
    def extension(self,information):
        if 'file_format' in information:
            self._extension= information['file_format']
        else:
            self._extension= None

    def copy_all_files(self):
        for file in self.srcbucket.objects.all():
            name = file.key
            self.nested_folders(name)
            if not self.extension or name.endswith(self.extension):
                dir_name=name.replace('/','\\')
                print(f"{self.destination_path}\\{name}")
            else:
                dir_name=name
            self.srcbucket.download_file(file.key, f"{self.destination_path}\\{dir_name}")
            if self.delete==1:
                self.delete_files_from_minio(name)


    def nested_folders(self,file_path):
        if '/' in file_path:
            path = file_path.split("/")
            dir = path[0]
            if not os.path.exists(self.destination_path+'\\'+dir):
                os.mkdir(self.destination_path+'\\'+dir)



    def delete_files_from_minio(self,file):
        self.s3.Object(self.bucket_name, file).delete()

    def create_directory(self):
        if not os.path.exists(self.destination_path):
            os.makedirs(self.destination_path)

'''x=MinioReader( {"name": "excel_test","minio_bucket":"test","local_path":"path\\to\\minio_bucket","destination_table":"test"
             ,"server_address":"bi2","file_format":".xlsx","database":"test","insert_strategy":"replace","delete_from_minio": 1
			 ,"exclude_columns":["id"]
             })'''