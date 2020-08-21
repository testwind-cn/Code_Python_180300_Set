#!coding:utf-8

import platform
import pathlib
import csv
from collections import OrderedDict


class CsvConfigData:
    __filename: str = ''
    __data_list: list = []
    __m_sys_id: int = -1
    __find_row: OrderedDict = None

    # 定义构造方法
    def __init__(self, filename):
        # csv.register_dialect('unixpwd', delimiter=':', quoting=csv.QUOTE_NONE)
        csv.register_dialect('pycsv', delimiter=',',quotechar='"', quoting=csv.QUOTE_ALL,
                         doublequote=False, escapechar='\\', skipinitialspace=True, lineterminator='\r\n')
        self.__filename = filename
        self.open_dict()

    def open(self):
        with open(self.__filename, 'r', newline='', encoding='utf-8') as f:
            reader = csv.reader(f, dialect='pycsv')
            self.__data_list = []
            for row in reader:
                self.__data_list.append(row)
                print(row)

    def write_dict(self):
        with open(self.__filename, 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = ['first_name', 'last_name']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames, dialect='pycsv')
            writer.writeheader()
            writer.writerow({'first_name': 'Baked22', 'last_name': 'Beans'})
            writer.writerow({'first_name': 'Lovely', 'last_name': 'Spam33'})
            writer.writerow({'first_name': 'Wonderful', 'last_name': 'Spam44'})

    def open_dict(self):
        try:
            with open(self.__filename, 'r', newline='', encoding='utf-8') as csvfile:
                reader = csv.DictReader(csvfile, dialect='pycsv')
                self.__data_list = []
                for row in reader:
                    self.__data_list.append(row)
                    # print(row)
        except:
            pass

    def open_dict_with_head(self, fieldnames):
        try:
            with open(self.__filename, 'r', newline='', encoding='utf-8') as csvfile:
                # fieldnames = ['first_name', 'last_name']
                reader = csv.DictReader(csvfile, fieldnames=fieldnames, dialect='pycsv')
                self.__data_list = []
                for row in reader:
                    self.__data_list.append(row)
                    # print(row)
        except:
            pass

    def __get_rows(self, key: str, value: str):
        ret_list: list = []
        d: OrderedDict
        for d in self.__data_list:
            if d.get(key, "") == value:
                ret_list.append(d)
        return ret_list

    def find_row(self, data_id:str, date_value: str = "",
                range1_name='range_start', range2_name='range_end',
                range1_default="1970-01-01", range2_default="2199-12-31"):
        d: OrderedDict
        data_list = self.__get_rows("id", data_id)
        for data in data_list:
            if date_value is None or len(date_value) <= 0:
                self.__find_row = data
                return
            else:
                start = data.get(range1_name, range1_default)
                end = data.get(range2_name, range2_default)
                if start <= date_value < end:
                    self.__find_row = data
                    return
        self.__find_row = None

    def has_row(self) -> bool:
        if self.__find_row is not None and len(self.__find_row) > 0:
            return True
        else:
            return False

    def get_row(self):
        return self.__find_row


    def get_data_str(self, key: str, default_value: str = ""):
        if self.__find_row is None or len(self.__find_row) <= 0:
            return default_value
        return self.__find_row.get(key, default_value)

    def get_data_int(self, key: str, default_value: int = 0):
        if self.__find_row is None or len(self.__find_row) <= 0:
            return default_value
        s = self.__find_row.get(key, default_value)
        if type(s) is int:
            return s
        if type(s) is str and s.isdecimal():
            return int(s)
        return default_value

    def get_data_bool(self, key: str, default_value: bool = False):
        if self.__find_row is None or len(self.__find_row) <= 0:
            return default_value
        s = self.__find_row.get(key, default_value)
        if type(s) is bool:
            return s
        if type(s) is int:
            return bool(s)
        if type(s) is str and s.isdecimal():
            return bool(int(s))
        return default_value


if __name__ == "__main__":
    job_row = data_row = location_row = {}

    job_info = CsvConfigData("conf_info/job_info.csv")
    sys_id: int = 1
    data_info = CsvConfigData("conf_info/data_info.csv")
    location_info = CsvConfigData("conf_info/location_info_"+str(sys_id)+".csv")

    job_row = job_info.get_row("2", "2099-12-30")
    print(job_row)

    if job_row is not None and len(job_row) > 0:
        data_id = job_row.get("data_id", "")
        location_id = job_row.get("location_id", "")
        if len(data_id) >= 0:
            data_row = data_info.get_row(data_id)
            print(data_row)
        if len(location_id) >= 0:
            location_row = location_info.get_row(location_id)
            print(location_row)

    if job_row is None or data_row is None or location_row is None or \
        len(job_row) <= 0 or len(data_row) <= 0 or len(location_row) <= 0:
        exit(-1)



