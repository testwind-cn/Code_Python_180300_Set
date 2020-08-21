#!coding:utf-8

import platform
import pathlib
from conf_csv import CsvConfigData
from py_tools.str_tool import StrTool
import datetime

class ConfigData:
    """
    p_name 参数名
    g_name 全局变量
    f_name 函数变量
    m_name 成员变量
    t_name 临时变量

    不再使用静态函数     @staticmethod
    """
    __m_is_test_mode: bool = False
    __m_project_id: int = 1
    __m_sys_id: int = -1
    __m_p_date: datetime.date
    __job_info: CsvConfigData
    __data_info: CsvConfigData
    __location_info: CsvConfigData
    __is_ready: bool = False

    def find_row(self, p_date: str = "", p_id: int = -1):
        if p_id > 0:
            self.__m_project_id = p_id

        self.__m_p_date = StrTool.get_the_date(p_date)

        self.__job_info.find_row(str(self.__m_project_id), self.__m_p_date.strftime("%Y-%m-%d"))  # "2", "2099-12-30"
        print(self.__job_info.get_row())

        if self.__job_info.has_row():
            data_id = self.__job_info.get_data_str("data_id")
            location_id = self.__job_info.get_data_str("location_id")
            if len(data_id) >= 0:
                self.__data_info.find_row(data_id)
                print(self.__data_info.get_row())
            if len(location_id) >= 0:
                self.__location_info.find_row(location_id)
                print(self.__location_info.get_row())

        if self.__job_info.has_row() and self.__data_info.has_row() and self.__location_info.has_row():
            self.__is_ready = True
        else:
            self.__is_ready = False

    def __init__(self, p_id: int = 0, p_date: str = "", p_is_test: bool = False):
        self.__m_is_test_mode = p_is_test
        self.__m_project_id = p_id
        self.get_sys_id()

        self.__job_info = CsvConfigData("/app/code/posflow_loader_2/conf_info/job_info.csv")
        self.__data_info = CsvConfigData("/app/code/posflow_loader_2/conf_info/data_info.csv")
        self.__location_info = CsvConfigData("/app/code/posflow_loader_2/conf_info/location_info_" + str(self.get_sys_id()) + ".csv")

        self.find_row(p_date)

    def get_p_date(self) -> str:        # partition folder date
        return (self.__m_p_date - datetime.timedelta(days=self.get_file_date_delta())).strftime("%Y-%m-%d")

    def get_f_date(self) -> str:        # file name date
        return (self.__m_p_date - datetime.timedelta(days=self.get_file_date_delta())).strftime("%Y%m%d")

    def is_test(self):
        return self.__m_is_test_mode

    def get_sys_id(self):
        if self.__m_sys_id < 0:
            if platform.platform().lower().startswith("linux") and not self.is_test():
                self.__m_sys_id = 0
            if platform.platform().lower().startswith("windows") and not self.is_test():
                self.__m_sys_id = 1
            if platform.platform().lower().startswith("linux") and self.is_test():
                self.__m_sys_id = 2
            if platform.platform().lower().startswith("windows") and self.is_test():
                self.__m_sys_id = 3
        return self.__m_sys_id

    #######################
    # 1、收银宝，3、资产流水，4、资产风险、5、资产流水，6、资产风险，7、保理流水，8、保理流水，9、保理风险
    def get_job_id(self):
        self.__job_info.get_data_int("id")

    def get_data_id(self):
        self.__data_info.get_data_int("id")

    def get_location_id(self):
        self.__location_info.get_data_int("id")

    def get_zip_name(self, p_date: str = ""):
        f_name1 = self.__data_info.get_data_str("file_zip_pre")
        f_name2 = self.__data_info.get_data_str("file_zip_ext")

        if len(f_name1) > 0 or len(f_name2) > 0:
            return f_name1 + p_date + f_name2
        else:
            return ""

    def get_ftp_name(self, p_date: str = ""):
        f_name1 = self.__data_info.get_data_str("file_ftp_pre")
        f_name2 = self.__data_info.get_data_str("file_ftp_ext")

        if len(f_name1) > 0 or len(f_name2) > 0:
            return f_name1 + p_date + f_name2
        else:
            return ""

    def get_file_name(self, p_date: str = ""):
        f_name1 = self.__data_info.get_data_str("file_data_pre")
        f_name2 = self.__data_info.get_data_str("file_data_ext")

        if len(f_name1) > 0 or len(f_name2) > 0:
            return f_name1 + p_date + f_name2
        else:
            return ""

    def get_table_name(self):
        return self.__data_info.get_data_str("hive_db") + "." + self.__data_info.get_data_str("hive_table")

    def get_has_partition(self):
        return self.__data_info.get_data_bool("has_partition", False)

    def get_hive_head(self) -> bool:
        # hive_head1 = "1",  # 1、有表头，get_hive_head = True, 需要删除, need_head = not get_hive_head = False
        # hive_head3 = "0",  # 0、无表头，get_hive_head = False, 不要删除, need_head = not get_hive_head = True
        return self.__data_info.get_data_bool("hive_head", False)

    def get_hive_add_date(self, p_date: str = ""):
        # hive_add_date_10 = "1",  # 1、要增加日期字段
        if self.__data_info.get_data_bool("hive_add_date", False):
            return p_date
        else:
            return ""

    def get_file_date_delta(self):
        return self.__data_info.get_data_int("file_date_delta", 0)

    def get_ftp_ip(self):
        return self.__data_info.get_data_str("ftp_ip")

    def get_ftp_port(self):
        return self.__data_info.get_data_str("ftp_port")

    def get_ftp_user(self):
        return self.__data_info.get_data_str("ftp_user")

    def get_ftp_pass(self):
        return self.__data_info.get_data_str("ftp_pass")

    def get_remote_path_ftp(self, p_date: str = ""):
        f_v1 = self.__data_info.get_data_str("remote_ftp_path")
        if self.__data_info.get_data_bool("remote_ftp_path_date", False):
            f_v1 = str(pathlib.PurePosixPath(f_v1).joinpath(p_date))
        return f_v1

    def get_hdfs_path(self):
        return self.__data_info.get_data_str("hdfs_dir")

    #######################

    def get_local_path_ftp(self):
        return self.__location_info.get_data_str("local_path_ftp")

    def get_zip_path(self):
        return self.__location_info.get_data_str("local_path_zip")

    def get_data_path(self):
        return self.__location_info.get_data_str("local_path_data")

    def get_utf8_path(self):
        return self.__location_info.get_data_str("local_path_utf8")

    #######################

    def test_date(self):
        return "20180901"

    def cdh_ip(self):
        return self.__location_info.get_data_str("cdh_ip")

    def cdh_user(self):
        return self.__location_info.get_data_str("cdh_user")

    def cdh_pass(self):
        return self.__location_info.get_data_str("cdh_pass")

    def hdfs_ip(self):
        return "http://" + self.__location_info.get_data_str("cdh_ip") + ":" + self.__location_info.get_data_str("hdfs_port")

    def hive_ip(self):
        return self.__location_info.get_data_str("hive_ip")

    def hive_port(self):
        return self.__location_info.get_data_int("hive_ip", 10000)

    def hive_user(self):
        return self.__location_info.get_data_str("hive_user")

    def hive_auth(self):
        return self.__location_info.get_data_str("hive_auth")

    def hive_test(self):
        return self.__location_info.get_data_str("hive_test")


if __name__ == "__main__":
    aaa = ConfigData( 7, "2019-04-08")

    print(aaa.is_test())
    print(aaa.get_sys_id())

    print(aaa.get_zip_name("szxc"))
    print(aaa.get_ftp_name("szxc"))
    print(aaa.get_file_name("szxc"))
    print(aaa.get_hive_add_date("szxc"))
    print(aaa.get_remote_path_ftp("szxc"))

    print(aaa.get_table_name())
    print(aaa.get_hive_head())
    print(aaa.get_ftp_ip())
    print(aaa.get_ftp_port())
    print(aaa.get_ftp_user())
    print(aaa.get_ftp_pass())
    print(aaa.get_hdfs_path())
    print(aaa.get_local_path_ftp())
    print(aaa.get_zip_path())
    print(aaa.get_data_path())
    print(aaa.get_utf8_path())
    print(aaa.test_date())
    print(aaa.hdfs_ip())
    print(aaa.hive_ip())
    print(aaa.hive_port())
    print(aaa.hive_user())
    print(aaa.hive_auth())


