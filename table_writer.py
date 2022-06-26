import pandas as pd
from databases import Database

####with given configs in the json file, copied data is inserted to the mentioned destination table####
####it also logs each insert for future checks
####the original file is moved into an archive folder with date/time added to the end of its name
class TableWriter():
    def __init__(self, information, log_table=None):
        self.log_table = 'file_import_logs'
        self.destination = information['destination_table']
        self.extension = information['file_format']
        self.insert_strategy = information['insert_strategy']
        self.file_name = information['file_name']
        self.folder_name = (information)
        self.file_path = (information)
        self.csv_has_header = (information)
        self.include_columns = (information)
        self.has_multiple_sheets = self.__has_multiple_sheets__()
        self.exclude_columns = (information)
        self.start_row = (information)
        self.include_only = (information)

        self.file_content, self.columns = self.__read_file__()
        self.__add_include_columns__()
        self.connection = Database(information['server_address'], information['database'])

    def __read_file__(self):
        if self.extension == '.xlsx':
            file_content, file_columns = self.__read_excel_file__()
        elif self.extension == '.csv':
            file_content, file_columns = self.__read_csv_file__()
        else:
            file_content, file_columns = 0, 0
        return file_content, file_columns

    def __read_excel_file__(self):
        if self.has_multiple_sheets == 0:
            file_content = pd.read_excel(self.file_path, skiprows=self.start_row - 1)
        else:
            workbook = pd.ExcelFile(self.file_path)
            sheets = workbook.sheet_names
            file_content = pd.concat([pd.read_excel(workbook, sheet_name=s)
                                     .assign(sheet_name=s) for s in sheets])
        file_content = file_content.dropna(axis=1, how='all')
        if self.include_only:
            file_content = file_content[self.include_only]
        columns = file_content.columns.values
        return file_content, columns

    def __read_csv_file__(self):
        if self.csv_has_header == 1:
            file_content = pd.read_csv(self.file_path, skiprows=self.start_row - 1)
        else:
            file_content = pd.read_csv(self.file_path, skiprows=self.start_row - 1, header=None)
        file_content = file_content.dropna(axis=1, how='all')
        if self.include_only:
            file_content = file_content[self.include_only]
        columns = file_content.columns.values
        return file_content, columns

    @property
    def folder_name(self):
        return self._folder_name

    @folder_name.setter
    def folder_name(self, information):
        if "folder_name" in information:
            self._folder_name = information["folder_name"]
        else:
            self._folder_name = None

    @property
    def file_path(self):
        return self._file_path

    @file_path.setter
    def file_path(self, information):
        self._file_path = information['local_path'] + '\\' + information['file_name']

    @property
    def csv_has_header(self):
        return self._csv_has_header

    @csv_has_header.setter
    def csv_has_header(self, information):
        if self.extension == '.csv' and 'has_header' in information:
            self._csv_has_header = information['has_header']
        else:
            self._csv_has_header = 1

    @property
    def include_columns(self):
        return self._include_columns

    @include_columns.setter
    def include_columns(self, information):
        if "include_column" in information:
            self._include_columns = information["include_column"]
        else:
            self._include_columns = None

    @property
    def exclude_columns(self):
        return self._exclude_columns

    @exclude_columns.setter
    def exclude_columns(self, information):
        if "exclude_columns" in information:
            self._exclude_columns= information["exclude_columns"]
        else:
            self._exclude_columns= None
    @property
    def start_row(self):
        return self._start_row
    @start_row.setter
    def start_row(self,information):
        if "start_row" in information:
            self._start_row= information["start_row"]
        else:
            self._start_row= 1

    @property
    def include_only(self):
        return self._include_only

    @include_only.setter
    def include_only(self,information):
        if "include_only" in information:
            self._include_only= information["include_only"]
        else:
            self._include_only= None

    def __has_multiple_sheets__(self):
        if self.extension == '.xlsx':
            xl = pd.ExcelFile(self.file_path)
            res = len(xl.sheet_names)
            if res > 1:
                self.include_columns.append('sheet_name')
                return 1
            else:
                return 0
        else:
            return 0


    def __add_include_columns__(self):

        if 'folder_name' in self.include_columns:
            self.__add_folder_name__()
        if 'file_name' in self.include_columns:
            self.__add_file_name__()
        if 'sheet_name' in self.include_columns:
            pass

    def __add_folder_name__(self):
        if self.folder_name:
            self.file_content['folder_name'] = self.folder_name

    def __add_file_name__(self):
        self.file_content['file_name'] = self.file_name

    def __set_include_columns_data__(self):
        if self.include_only:
            return self.file_content[self.include_only]

    def insert_to_table(self):
        self.connection.insert_dataframe(self.file_content, self.destination, self.insert_strategy)

    def insert_log(self):
        meta_data = {'insert_count': [self.file_content.shape[0]], 'destination_table': [self.destination]}
        print(meta_data)
        if 'folder_name' in self.include_columns:
            meta_data['source_folder_name'] = [self.folder_name]
        if 'file_name' in self.include_columns:
            meta_data['source_file_name'] = [self.file_name]
        log_df = pd.DataFrame.from_dict(meta_data)
        self.connection.insert_dataframe(log_df, self.log_table, 'append')


'''
x = TableWriter(
   {"name": "excel_test","minio_bucket":"test","local_path":"path\\to\\minio_bucket","destination_table":"test"
             ,"server_address":"bi2","file_format":".xlsx","database":"test","insert_strategy":"replace","delete_from_minio": 1
			 ,"exclude_columns":["id"]
             })
x.insert_to_table()
'''
