import sys

from minio_reader import MinioReader
from utilities import read_config_file, write_foldered_files_to_table, write_files_to_table

"""
This code is supposed to run on a windows machine using cmd, the run command must also include the value of the config name which is stored in a JSON config file
It is requested to run the script from a SQL server instance job agent.
The policy is to store the data on server as well.
It copies the files into server, loads the data into a dataframe, do some transformations (add extra columns like file name or foldername, exclude mentioned columns,...)
and insert the data in a given destionation table address.
The Config file contains all needed data to load the data into table
"""

def read_insert_minio_bucket(config_name):
    config=read_config_file()
    for bucket in config:
        if bucket['name']==config_name:
            reader = MinioReader(bucket)
            reader.copy_all_files()
            write_foldered_files_to_table(bucket)
            write_files_to_table(bucket)
            break

if __name__ == "__main__":
    read_insert_minio_bucket(sys.argv[1])

#read_insert_minio_bucket("test")
