import json
import os
import shutil

from minio_reader import MinioReader
from table_writer import TableWriter
from datetime import datetime


def list_directories(path):
  return   [ name for name in os.listdir(path) if os.path.isdir(os.path.join(path, name)) ]

def list_all_files_with_extension(path,extension):
    files_list = [x for x in os.listdir(path) if x.endswith(extension)]
    return files_list


def write_foldered_files_to_table(information):
    for directory in list_directories(information['local_path']):
        file_info = information.copy()
        file_info['folder_name'] = directory
        file_info['local_path'] = file_info['local_path'] + f'\\{directory}'
        for file in list_all_files_with_extension(information['local_path'] + f'\\{directory}',
                                                  information["file_format"]):
            file_info['file_name'] = file
            write_from_local_sql(file_info)

def write_files_to_table(information):
    for file in list_all_files_with_extension(information['local_path'] , information["file_format"]):
        print(file)
        file_info = information.copy()
        file_info['file_name'] = file
        write_from_local_sql(file_info)

def write_from_local_sql(information):
    writer = TableWriter(information)
    writer.insert_to_table()
    writer.insert_log()
    archive_files(information)

def read_config_file(config_path=r'config_file_path\minio_information_reader.json'):
    with open(config_path, 'r') as JSON:
        return json.load(JSON)

def archive_files(information):
    destination_path=create_directory(information)
    file_path=information['local_path']+'/'+information['file_name']
    new_name=information['file_name'].replace(information['file_format'],'')+'_archive_'+\
             datetime.today().strftime('%Y-%m-%d %H-%M-%S')+'.'+information['file_format']
    new_path=information['local_path']+'/'+new_name
    os.rename(file_path,new_path)
    shutil.move(new_path, destination_path)

def create_directory(information,dir_name='archive'):
    path=information['local_path'].replace(information['minio_bucket'],dir_name+'_'+information['minio_bucket'])
    if not os.path.exists(path):
        os.makedirs(path)
    return path