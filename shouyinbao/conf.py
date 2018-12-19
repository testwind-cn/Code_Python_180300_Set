#!coding:utf-8

import platform


class ConfigData:
    class RealData:
        data: dict = {}

        def __init__(self, ** kwargs):
            np = len(kwargs)
            print(np)
            self.data = kwargs

        def get_arg(self, name: str='', default=''):
            if type(self.data) is not dict:
                return ''
            return self.data.get(name, default)

    @staticmethod
    def get_data(name, default=''):
        the_data: [ConfigData.RealData] =\
            [ConfigData.RealData(root_path="/home/data/SYB/",
                                 unzip_dir="/home/data/posflow/shouyinbao/UNZIP/",
                                 decode_dir="/home/data/posflow/shouyinbao/UTF8/",
                                 hdfs_dir="/data/posflow/shouyinbao/UTF8/",
                                 file_pre1='t1_trxrecord_',
                                 file_ext1="_V2.zip",
                                 file_ext2="_V2.csv",
                                 test_date="20181101",
                                 hdfs_ip="http://10.91.1.20:50070",
                                 hive_ip="10.91.1.20",
                                 hive_port='10000',
                                 hive_user="root",
                                 hive_auth="PLAIN",
                                 hive_test="select * from rds.area",
                                 hive_table="posflow.t1_trxrecprd_V2",
                                 allinpay_ftp_ip_bl="172.31.130.14",
                                 allinpay_ftp_ip_zc="172.31.71.71",
                                 allinpay_ftp_port_bl="22",
                                 allinpay_ftp_port_zc="12306",
                                 allinpay_ftp_user_bl="root",
                                 allinpay_ftp_user_zc="tlbposmcht",
                                 allinpay_ftp_pass_bl="Redhat@2016",
                                 allinpay_ftp_pass_zc="T3beTUxIMq",
                                 allinpay_ftp_folder_bl="/ftpdata/thblposloan/posflow/",
                                 allinpay_ftp_folder_zc="/upload/",
                                 allinpay_data_bl="/home/data/posflow/allinpay_data_bl/",
                                 allinpay_data_zc="/home/data/posflow/allinpay_data_zc/",
                                 allinpay_utf8_bl="/home/data/posflow/allinpay_utf8_bl/",
                                 allinpay_utf8_zc="/home/data/posflow/allinpay_utf8_zc/"),

             ConfigData.RealData(root_path="D:\\下载\\TestData\\SYB\\",
                                 unzip_dir="D:\\下载\\TestData\\UNZIP\\",
                                 decode_dir="D:\\下载\\TestData\\UTF8\\",
                                 hdfs_dir="/data/posflow/shouyinbao/UTF8/",
                                 file_pre1='t1_trxrecord_',
                                 file_ext1="_V2.zip",
                                 file_ext2="_V2.csv",
                                 test_date="20181101",
                                 hdfs_ip="http://10.91.1.20:50070",
                                 hive_ip="10.91.1.20",
                                 hive_port='10000',
                                 hive_user="root",
                                 hive_auth="PLAIN",
                                 hive_test="select * from rds.area",
                                 hive_table="posflow.t1_trxrecprd_V2",
                                 allinpay_ftp_ip_bl="172.31.130.14",
                                 allinpay_ftp_ip_zc="172.31.71.71",
                                 allinpay_ftp_port_bl="22",
                                 allinpay_ftp_port_zc="12306",
                                 allinpay_ftp_user_bl="root",
                                 allinpay_ftp_user_zc="tlbposmcht",
                                 allinpay_ftp_pass_bl="Redhat@2016",
                                 allinpay_ftp_pass_zc="T3beTUxIMq",
                                 allinpay_ftp_folder_bl="/ftpdata/thblposloan/posflow/",
                                 allinpay_ftp_folder_zc="/upload/",
                                 allinpay_data_bl="D:\\下载\\posflow\\allinpay_data_bl\\",
                                 allinpay_data_zc="D:\\下载\\posflow\\allinpay_data_zc\\",
                                 allinpay_utf8_bl="D:\\下载\\posflow\\allinpay_utf8_bl\\",
                                 allinpay_utf8_zc="D:\\下载\\posflow\\allinpay_utf8_zc\\"),

             ConfigData.RealData(root_path="/media/bd/data/toshiba/data/SYB/",
                                 unzip_dir="/home/bd/桌面/201811_flow/bl_shouyinbao/UNZIP/",
                                 decode_dir="/home/bd/桌面/201811_flow/bl_shouyinbao/UTF8/",
                                 hdfs_dir="/shouyinbao/bl_shouyinbao/UTF8/",
                                 file_pre1='t1_trxrecord_',
                                 file_ext1="_V2.zip",
                                 file_ext2="_V2.csv",
                                 test_date="20181101",
                                 hdfs_ip="http://10.2.201.197:50070",
                                 hive_ip="10.2.201.197",
                                 hive_port='10000',
                                 hive_user="root",
                                 hive_auth="PLAIN",
                                 hive_test="select * from test.test1",
                                 hive_table='test.t1_trxrecprd_v2_bl',
                                 allinpay_ftp_ip_bl="172.31.130.14",
                                 allinpay_ftp_ip_zc="172.31.71.71",
                                 allinpay_ftp_port_bl="22",
                                 allinpay_ftp_port_zc="12306",
                                 allinpay_ftp_user_bl="root",
                                 allinpay_ftp_user_zc="tlbposmcht",
                                 allinpay_ftp_pass_bl="Redhat@2016",
                                 allinpay_ftp_pass_zc="T3beTUxIMq",
                                 allinpay_ftp_folder_bl="/ftpdata/thblposloan/posflow/",
                                 allinpay_ftp_folder_zc="/upload/",
                                 allinpay_data_bl="/home/data/posflow/allinpay_data_bl/",
                                 allinpay_data_zc="/home/data/posflow/allinpay_data_zc/",
                                 allinpay_utf8_bl="/home/data/posflow/allinpay_utf8_bl/",
                                 allinpay_utf8_zc="/home/data/posflow/allinpay_utf8_zc/"),

             ConfigData.RealData(root_path='D:\\DATA\\SYB\\',
                                 unzip_dir='D:\\DATA\\UNZIP\\',
                                 decode_dir='D:\\DATA\\UTF8\\',
                                 hdfs_dir="/shouyinbao/bl_shouyinbao/UTF8/",
                                 file_pre1='t1_trxrecord_',
                                 file_ext1="_V2.zip",
                                 file_ext2="_V2.csv",
                                 test_date="20181101",
                                 hdfs_ip="http://10.2.201.197:50070",
                                 hive_ip="10.2.201.197",
                                 hive_port='10000',
                                 hive_user="root",
                                 hive_auth="PLAIN",
                                 hive_test="select * from test.test1",
                                 hive_table='test.t1_trxrecprd_v2_bl',
                                 allinpay_ftp_ip_bl="172.31.130.14",
                                 allinpay_ftp_ip_zc="172.31.71.71",
                                 allinpay_ftp_port_bl="22",
                                 allinpay_ftp_port_zc="12306",
                                 allinpay_ftp_user_bl="root",
                                 allinpay_ftp_user_zc="tlbposmcht",
                                 allinpay_ftp_pass_bl="Redhat@2016",
                                 allinpay_ftp_pass_zc="T3beTUxIMq",
                                 allinpay_ftp_folder_bl="/ftpdata/thblposloan/posflow/",
                                 allinpay_ftp_folder_zc="/upload/",
                                 allinpay_data_bl="/home/data/posflow/allinpay_data_bl/",
                                 allinpay_data_zc="/home/data/posflow/allinpay_data_zc/",
                                 allinpay_utf8_bl="/home/data/posflow/allinpay_utf8_bl/",
                                 allinpay_utf8_zc="/home/data/posflow/allinpay_utf8_zc/")
             ]

        pid = ConfigData.get_id()
        if pid < len(the_data):
            return the_data[pid].get_arg(name=name, default=default)

    @staticmethod
    def is_test():
        istest = False
        # istest = True
        return istest

    @staticmethod
    def get_id():
        if platform.platform().lower().startswith("linux") and not ConfigData.is_test():
            return 0
        if platform.platform().lower().startswith("windows") and not ConfigData.is_test():
            return 1
        if platform.platform().lower().startswith("linux") and ConfigData.is_test():
            return 2
        if platform.platform().lower().startswith("windows") and ConfigData.is_test():
            return 3
        return 0

    @staticmethod
    def root_path(isBaoli=True):
        return ConfigData.get_data("root_path")

    @staticmethod
    def unzip_dir(isBaoli=True):
        return ConfigData.get_data("unzip_dir")

    @staticmethod
    def decode_dir(isBaoli=True):
        return ConfigData.get_data("decode_dir")

    @staticmethod
    def hdfs_dir(isBaoli=True):
        return ConfigData.get_data("hdfs_dir")

    @staticmethod
    def file_pre1(isBaoli=True):
        return ConfigData.get_data("file_pre1")

    @staticmethod
    def file_ext1(isBaoli=True):
        return ConfigData.get_data("file_ext1")

    @staticmethod
    def file_ext2(isBaoli=True):
        return ConfigData.get_data("file_ext2")

    @staticmethod
    def test_date(isBaoli=True):
        return ConfigData.get_data("test_date")

    @staticmethod
    def hdfs_ip(isBaoli=True):
        return ConfigData.get_data("hdfs_ip")

    @staticmethod
    def hive_ip():
        return ConfigData.get_data("hive_ip")

    @staticmethod
    def hive_port():
        s = ConfigData.get_data("hive_port")
        if type(s) is int:
            return s
        if type(s) is str and s.isdecimal():
            return int(s)
        return 10000

    @staticmethod
    def hive_user():
        return ConfigData.get_data("hive_user")

    @staticmethod
    def hive_auth():
        return ConfigData.get_data("hive_auth")

    @staticmethod
    def hive_test():
        return ConfigData.get_data("hive_test")

    @staticmethod
    def hive_table(isBaoli=True):
        return ConfigData.get_data("hive_table")
