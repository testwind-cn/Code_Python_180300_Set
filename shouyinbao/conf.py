#!coding:utf-8

import platform


class ConfigData:
    """
    不再使用静态函数     @staticmethod
    """
    is_test_mode = False

    class RealData:
        data: dict = {}

        def __init__(self, ** kwargs):
            # np = len(kwargs)
            # print(np)
            self.data = kwargs

        def key_in(self, key: str) -> bool:
            if type(self.data) is not dict:
                return False
            return key in self.data

        def get_arg(self, name: str='', default=''):
            if type(self.data) is not dict:
                return ''
            return self.data.get(name, default)

    the_same_data: RealData = \
        RealData(file_pre1='t1_trxrecord_',
                 file_ext1="_V2.zip",
                 file_ext2="_V2.csv",
                 file_ext3="_loginfo_rsp.txt",                  # "20181101_loginfo_rsp.txt"
                 file_ext4="_loginfo_rsp_agt.txt",              # "20181101_loginfo_rsp_agt.txt"
                 file_ext5="_rxinfo_rsp.txt",
                 file_ext6="_rxinfo_rsp_agt.txt",
                 test_date="20181205",
                 hive_table="posflow.t1_trxrecprd_v2",
                 hive_table3="posflow.loginfo_rsp_zc",
                 hive_table4="posflow.loginfo_rsp_agt_zc",
                 hive_table5="posflow.rxinfo_rsp_zc",
                 hive_table6="posflow.rxinfo_rsp_agt_zc",
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
                 hdfs_dir_syb="/data/posflow/shouyinbao/UTF8/",
                 hdfs_dir_bl="/data/posflow/allinpay_utf8_bl/",
                 hdfs_dir_zc="/data/posflow/allinpay_utf8_zc/")

    the_diff_data: [RealData] = \
        [RealData(root_path="/home/data/SYB/",
                  unzip_dir="/home/data/posflow/shouyinbao/UNZIP/",
                  decode_dir="/home/data/posflow/shouyinbao/UTF8/",
                  hdfs_ip="http://10.91.1.100:50070",
                  hive_ip="10.91.1.100",
                  hive_port='10000',
                  hive_user="root",
                  hive_auth="PLAIN",
                  hive_test="select * from rds.area",
                  allinpay_data_bl="/home/data/posflow/allinpay_data_bl/",
                  allinpay_data_zc="/home/data/posflow/allinpay_data_zc/",
                  allinpay_utf8_bl="/home/data/posflow/allinpay_utf8_bl/",
                  allinpay_utf8_zc="/home/data/posflow/allinpay_utf8_zc/"),

         RealData(root_path="D:\\下载\\posflow\\SYB\\",
                  unzip_dir="D:\\下载\\posflow\\shouyinbao\\UNZIP\\",
                  decode_dir="D:\\下载\\posflow\\shouyinbao\\UTF8\\",
                  hdfs_ip="http://10.91.1.100:50070",
                  hive_ip="10.91.1.100",
                  hive_port='10000',
                  hive_user="root",
                  hive_auth="PLAIN",
                  hive_test="select * from rds.area",
                  allinpay_data_bl="D:\\下载\\posflow\\allinpay_data_bl\\",
                  allinpay_data_zc="D:\\下载\\posflow\\allinpay_data_zc\\",
                  allinpay_utf8_bl="D:\\下载\\posflow\\allinpay_utf8_bl\\",
                  allinpay_utf8_zc="D:\\下载\\posflow\\allinpay_utf8_zc\\"),

         RealData(root_path="/media/bd/data/toshiba/data/SYB/",
                  unzip_dir="/home/bd/桌面/20181201_test/posflow/shouyinbao/UNZIP/",
                  decode_dir="/home/bd/桌面/20181201_test/posflow/shouyinbao/UTF8/",
                  hdfs_ip="http://10.2.201.197:50070",
                  hive_ip="10.2.201.197",
                  hive_port='10000',
                  hive_user="root",
                  hive_auth="PLAIN",
                  hive_test="select * from test.test1",
                  allinpay_data_bl="/home/bd/桌面/20181201_test/posflow/allinpay_data_bl/",
                  allinpay_data_zc="/home/bd/桌面/20181201_test/posflow/allinpay_data_zc/",
                  allinpay_utf8_bl="/home/bd/桌面/20181201_test/posflow/allinpay_utf8_bl/",
                  allinpay_utf8_zc="/home/bd/桌面/20181201_test/posflow/allinpay_utf8_zc/"),

         RealData(root_path='D:\\DATA\\SYB\\',
                  unzip_dir='D:\\DATA\\posflow\\shouyinbao\\UNZIP\\',
                  decode_dir='D:\\DATA\\posflow\\shouyinbao\\UTF8\\',
                  hdfs_ip="http://10.2.201.197:50070",
                  hive_ip="10.2.201.197",
                  hive_port='10000',
                  hive_user="root",
                  hive_auth="PLAIN",
                  hive_test="select * from test.test1",
                  allinpay_data_bl="D:\\DATA\\posflow\\allinpay_data_bl\\",
                  allinpay_data_zc="D:\\DATA\\posflow\\allinpay_data_zc\\",
                  allinpay_utf8_bl="D:\\DATA\\posflow\\allinpay_utf8_bl\\",
                  allinpay_utf8_zc="D:\\DATA\\posflow\\allinpay_utf8_zc\\")
         ]

    def __init__(self, is_test: bool=False):
        self.is_test_mode = is_test

    def get_data(self, name, default=''):
        if self.the_same_data.key_in(name):
            return self.the_same_data.get_arg(name=name, default=default)
        pid = self.get_id()
        if pid < len(self.the_diff_data):
            return self.the_diff_data[pid].get_arg(name=name, default=default)

    def is_test(self):
        return self.is_test_mode

    def get_id(self):
        if platform.platform().lower().startswith("linux") and not self.is_test():
            return 0
        if platform.platform().lower().startswith("windows") and not self.is_test():
            return 1
        if platform.platform().lower().startswith("linux") and self.is_test():
            return 2
        if platform.platform().lower().startswith("windows") and self.is_test():
            return 3
        return 0

    def root_path(self, isBaoli=True):
        return self.get_data("root_path")

    def unzip_dir(self, isBaoli=True):
        return self.get_data("unzip_dir")

    def decode_dir(self, isBaoli=True):
        return self.get_data("decode_dir")

    def hdfs_dir_syb(self, isBaoli=True):
        return self.get_data("hdfs_dir_syb")

    def file_pre1(self, isBaoli=True):
        return self.get_data("file_pre1")

    def file_ext1(self, isBaoli=True):
        return self.get_data("file_ext1")

    def file_ext2(self, isBaoli=True):
        return self.get_data("file_ext2")

    def test_date(self, isBaoli=True):
        return self.get_data("test_date")

    def hdfs_ip(self, isBaoli=True):
        return self.get_data("hdfs_ip")

    def hive_ip(self):
        return self.get_data("hive_ip")

    def hive_port(self):
        s = self.get_data("hive_port")
        if type(s) is int:
            return s
        if type(s) is str and s.isdecimal():
            return int(s)
        return 10000

    def hive_user(self):
        return self.get_data("hive_user")

    def hive_auth(self):
        return self.get_data("hive_auth")

    def hive_test(self):
        return self.get_data("hive_test")

    def hive_table(self, isBaoli=True):
        return self.get_data("hive_table")
