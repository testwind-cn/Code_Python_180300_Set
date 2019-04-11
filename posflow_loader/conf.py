#!coding:utf-8

import platform
import pathlib


class ConfigData:
    """
    p_name 参数名
    g_name 全局变量
    f_name 函数变量
    m_name 成员变量
    t_name 临时变量

    不再使用静态函数     @staticmethod
    """
    m_is_test_mode: bool = False
    m_project_id: int = 1
    m_data_id: int = 0
    m_sys_id: int = -1

    class RealData:
        m_data: dict = {}

        def __init__(self, **p_kwargs):
            # np = len(p_kwargs)
            # print(np)
            self.m_data = p_kwargs

        def key_in(self, p_key: str) -> bool:
            if type(self.m_data) is not dict:
                return False
            return p_key in self.m_data

        def get_arg(self, p_name: str = '', p_default=''):
            if type(self.m_data) is not dict:
                return ''
            return self.m_data.get(p_name, p_default)

    m_same_data: RealData = \
        RealData(test_date="20181205",

                 hive_table1="rds_posflow.t1_trxrecprd_v2",  # 表名只能小写
                 hive_table2="rds_posflow.t1_trxrecprd_v2_tmp",  # 表名只能小写
                 hive_table3="rds_posflow.loginfo_rsp_zc",
                 hive_table4="rds_posflow.rxinfo_rsp_zc",
                 hive_table5="rds_posflow.loginfo_rsp_agt_zc",
                 hive_table6="rds_posflow.rxinfo_rsp_agt_zc",
                 hive_table7="rds_posflow.loginfo_rsp_bl",
                 hive_table8="rds_posflow.loginfo_rsp_agt_bl",
                 hive_table9="rds_posflow.rxinfo_rsp_bl",
                 hive_table10="rds_posflow.branch_apms_bl",

                 hive_head1="1",    # 1、有表头，需要删除, need_head=False
                 hive_head2="1",
                 hive_head3="0",    # 0、无表头，不要删除, need_head=True
                 hive_head4="0",
                 hive_head5="0",
                 hive_head6="0",
                 hive_head7="1",
                 hive_head8="1",
                 hive_head9="0",
                 hive_head10="1",

                 hive_add_date_10="1",   # 在最后一列，追加文件日期

                 file_date_delta1="1",  # 时间偏移一天，2019-1-9 处理 2019-1-8 ， 没有就是0
                 file_date_delta2="1",
                 file_date_delta7="1",  # 时间偏移一天，2019-1-9 处理 2019-1-8 ， 没有就是0
                 file_date_delta8="1",

                 file_date_time3="3:50",
                 file_date_time4="3:50",
                 file_date_time5="13:00",
                 file_date_time6="13:00",
                 file_date_time7="3:40",
                 file_date_time8="10:40",
                 file_date_time9="3:40",
                 file_date_time10="22:30",

                 ftp_ip_3="172.31.71.71",
                 ftp_ip_4="=ftp_ip_3",
                 ftp_ip_5="=ftp_ip_3",
                 ftp_ip_6="=ftp_ip_3",
                 ftp_ip_7="172.31.130.14",
                 ftp_ip_8="=ftp_ip_7",
                 ftp_ip_9="=ftp_ip_7",
                 ftp_ip_10="=ftp_ip_7",

                 ftp_port_3="12306",
                 ftp_port_4="=ftp_port_3",
                 ftp_port_5="=ftp_port_3",
                 ftp_port_6="=ftp_port_3",
                 ftp_port_7="22",
                 ftp_port_8="=ftp_port_7",
                 ftp_port_9="=ftp_port_7",
                 ftp_port_10="=ftp_port_7",

                 ftp_user_3="tlbposmcht",
                 ftp_user_4="=ftp_user_3",
                 ftp_user_5="=ftp_user_3",
                 ftp_user_6="=ftp_user_3",
                 ftp_user_7="root",
                 ftp_user_8="=ftp_user_7",
                 ftp_user_9="=ftp_user_7",
                 ftp_user_10="=ftp_user_7",

                 ftp_pass_3="T3beTUxIMq",
                 ftp_pass_4="=ftp_pass_3",
                 ftp_pass_5="=ftp_pass_3",
                 ftp_pass_6="=ftp_pass_3",
                 ftp_pass_7="Redhat@2016",
                 ftp_pass_8="=ftp_pass_7",
                 ftp_pass_9="=ftp_pass_7",
                 ftp_pass_10="=ftp_pass_7",

                 remote_path_ftp_3="/upload/",
                 remote_path_ftp_4="=remote_path_ftp_3",
                 remote_path_ftp_5="=remote_path_ftp_3",
                 remote_path_ftp_6="=remote_path_ftp_3",
                 remote_path_ftp_7="/ftpdata/thblposloan/posflow/",
                 remote_path_ftp_8="=remote_path_ftp_7",
                 remote_path_ftp_9="/ftpdata/thblposloan/sftp/data/thblposloan/traderisk/",
                 remote_path_ftp_10="/ftpdata/thblposloan/merchantsinfo/",

                 remote_date_ftp_7='1',
                 remote_date_ftp_8='1',
                 remote_date_ftp_9='1',
                 remote_date_ftp_10='1',

                 file_pre1='t1_trxrecord_',
                 file_ftp1="V2.zip",               # "t1_trxrecord_20190108_V2.zip"
                 file_zip1="V2.zip",               # "t1_trxrecord_20190108_V2.zip"
                 file_ext1="_V2.csv",               # "t1_trxrecord_20190108_V2.csv"

                 file_pre2='t1_trxrecord_',
                 file_ftp2="V2.zip",  # "t1_trxrecord_20190108_V2.zip"
                 file_zip2="V2.zip",  # "t1_trxrecord_20190108_V2.zip"
                 file_ext2="_V2.csv",

                 # 2019-1-8 03:48 来,无表头 GB      # /upload/20190108.zip
                 file_ftp3=".zip",                  # "20190108.zip"
                 file_zip3=".zip",                  # "20190108.zip"
                 file_ext3="_loginfo_rsp.txt",      # "20190108_loginfo_rsp.txt"

                 file_ftp4=".zip",                  # "20190108.zip"
                 file_zip4=".zip",                  # "20190108.zip"
                 file_ext4="_rxinfo_rsp.txt",       # "20190108_rxinfo_rsp.txt"

                 # 2019-1-8 12:55 来,无表头 GB      # /upload/20190108_agt.zip
                 file_ftp5="_agt.zip",              # "20190108_agt.zip"
                 file_zip5="_agt.zip",              # "20190108_agt.zip"
                 file_ext5="_loginfo_rsp_agt.txt",  # "20190108_loginfo_rsp_agt.txt"

                 file_ftp6="_agt.zip",              # "20190108_agt.zip"
                 file_zip6="_agt.zip",              # "20190108_agt.zip"
                 file_ext6="_rxinfo_rsp_agt.txt",   # "20190108_rxinfo_rsp_agt.txt"

                 # 2019-1-8 03:39 来,有表头 GB      # /ftpdata/thblposloan/posflow/20190107/20190107_loginfo_rsp_bl_new.csv
                 file_ftp7="_loginfo_rsp_bl_new.csv",
                 file_ext7="_loginfo_rsp_bl_new.csv",

                 # 2019-1-8 10:39 来,有表头 GB      # /ftpdata/thblposloan/posflow/20190107/20190107_rsp_agt_bl_new.del
                 file_ftp8="_rsp_agt_bl_new.del",
                 file_ext8="_rsp_agt_bl_new.del",

                 # 2019-1-8 03:31 来,无表头 GB      # /ftpdata/thblposloan/sftp/data/thblposloan/traderisk/20190108/20190108_rxinfo_rsp_bl.txt
                 file_ftp9="_rxinfo_rsp_bl.txt",
                 file_ext9="_rxinfo_rsp_bl.txt",

                 # 2019-1-8 22:25 来,有表头 GB      # /ftpdata/thblposloan/merchantsinfo/20190108/Branch_APMS_2nd_20190108.txt
                 file_pre10='Branch_APMS_2nd_',
                 file_ftp10=".txt",                 # "Branch_APMS_2nd_20190108.txt"
                 file_ext10=".txt",                 # "Branch_APMS_2nd_20190108.txt"

                 hdfs_dir_1="/data/posflow/shouyinbao_utf8/",
                 hdfs_dir_2="/data/posflow/shouyinbao_tmp_utf8/",
                 hdfs_dir_3="/data/posflow/allinpay_utf8_zc/",
                 hdfs_dir_4="=hdfs_dir_3",
                 hdfs_dir_5="=hdfs_dir_3",
                 hdfs_dir_6="=hdfs_dir_3",
                 hdfs_dir_7="/data/posflow/allinpay_utf8_bl/",
                 hdfs_dir_8="=hdfs_dir_7",
                 hdfs_dir_9="=hdfs_dir_7",
                 hdfs_dir_10="=hdfs_dir_7",

                 local_path_ftp_4="=local_path_ftp_3",
                 local_path_zip_4="=local_path_zip_3",
                 local_path_data_4="local_path_data_3",
                 local_path_utf8_4="local_path_utf8_3",

                 local_path_ftp_5="=local_path_ftp_3",
                 local_path_zip_5="=local_path_zip_3",
                 local_path_data_5="local_path_data_3",
                 local_path_utf8_5="local_path_utf8_3",

                 local_path_ftp_6="=local_path_ftp_3",
                 local_path_zip_6="=local_path_zip_3",
                 local_path_data_6="local_path_data_3",
                 local_path_utf8_6="local_path_utf8_3",

                 local_path_ftp_8="=local_path_ftp_7",
                 local_path_data_8="local_path_data_7",
                 local_path_utf8_8="local_path_utf8_7",

                 local_path_ftp_9="=local_path_ftp_7",
                 local_path_data_9="local_path_data_7",
                 local_path_utf8_9="local_path_utf8_7",

                 local_path_ftp_10="=local_path_ftp_7",
                 local_path_data_10="local_path_data_7",
                 local_path_utf8_10="local_path_utf8_7")

    m_diff_data: [RealData] = \
        [RealData(cdh_ip="10.91.1.100",
                  cdh_user="root",
                  cdh_pass="Redhat@2016",
                  hdfs_port="50070",
                  hive_ip="10.91.1.100",
                  hive_port='10000',
                  hive_user="admin",
                  hive_auth="PLAIN",
                  hive_test="select * from rds.area",

                  local_path_ftp_1="/home/data/SYB/",
                  local_path_zip_1="/home/data/SYB/",
                  local_path_data_1="/home/data/posflow/shouyinbao_data/",
                  local_path_utf8_1="/home/data/posflow/shouyinbao_utf8/",

                  local_path_ftp_2="/home/data/SYB/",
                  local_path_zip_2="/home/data/SYB/",
                  local_path_data_2="/home/data/posflow/shouyinbao_tmp_data/",
                  local_path_utf8_2="/home/data/posflow/shouyinbao_tmp_utf8/",

                  local_path_ftp_3="/home/data/posflow/allinpay_zip_zc/",
                  local_path_zip_3="/home/data/posflow/allinpay_zip_zc/",
                  local_path_data_3="/home/data/posflow/allinpay_data_zc/",
                  local_path_utf8_3="/home/data/posflow/allinpay_utf8_zc/",

                  local_path_ftp_7="/home/data/posflow/allinpay_data_bl/",
                  local_path_data_7="/home/data/posflow/allinpay_data_bl/",
                  local_path_utf8_7="/home/data/posflow/allinpay_utf8_bl/"),

         RealData(cdh_ip="10.91.1.100",
                  cdh_user="root",
                  cdh_pass="Redhat@2016",
                  hdfs_port="50070",
                  hive_ip="10.91.1.100",
                  hive_port='10000',
                  hive_user="root",
                  hive_auth="PLAIN",
                  hive_test="select * from rds.area",

                  local_path_ftp_1="D:\\下载\\posflow\\SYB\\",
                  local_path_zip_1="D:\\下载\\posflow\\SYB\\",
                  local_path_data_1="D:\\下载\\posflow\\shouyinbao_data\\",
                  local_path_utf8_1="D:\\下载\\posflow\\shouyinbao_utf8\\",

                  local_path_ftp_3="D:\\下载\\posflow\\allinpay_zip_zc\\",
                  local_path_zip_3="D:\\下载\\posflow\\allinpay_zip_zc\\",
                  local_path_data_3="D:\\下载\\posflow\\allinpay_data_zc\\",
                  local_path_utf8_3="D:\\下载\\posflow\\allinpay_utf8_zc\\",

                  local_path_ftp_7="D:\\下载\\posflow\\allinpay_data_bl\\",
                  local_path_data_7="D:\\下载\\posflow\\allinpay_data_bl\\",
                  local_path_utf8_7="D:\\下载\\posflow\\allinpay_utf8_bl\\"),

         RealData(cdh_ip="10.2.201.197",
                  cdh_user="root",
                  cdh_pass="Redhat@2016",
                  hdfs_port="50070",
                  hive_ip="10.2.201.197",
                  hive_port='10000',
                  hive_user="root",
                  hive_auth="PLAIN",
                  hive_test="select * from test.test1",

                  local_path_ftp_1="/media/bd/data/toshiba/data/SYB/",
                  local_path_zip_1="/media/bd/data/toshiba/data/SYB/",
                  local_path_data_1="/home/bd/桌面/20181201_test/posflow/shouyinbao_data/",
                  local_path_utf8_1="/home/bd/桌面/20181201_test/posflow/shouyinbao_utf8/",

                  local_path_ftp_3="/home/bd/桌面/20181201_test/posflow/allinpay_zip_zc/",
                  local_path_zip_3="/home/bd/桌面/20181201_test/posflow/allinpay_zip_zc/",
                  local_path_data_3="/home/bd/桌面/20181201_test/posflow/allinpay_data_zc/",
                  local_path_utf8_3="/home/bd/桌面/20181201_test/posflow/allinpay_utf8_zc/",

                  local_path_ftp_7="/home/bd/桌面/20181201_test/posflow/allinpay_data_bl/",
                  local_path_data_7="/home/bd/桌面/20181201_test/posflow/allinpay_data_bl/",
                  local_path_utf8_7="/home/bd/桌面/20181201_test/posflow/allinpay_utf8_bl/"),

         RealData(cdh_ip="10.2.201.197",
                  cdh_user="root",
                  cdh_pass="Redhat@2016",
                  hdfs_port="50070",
                  hive_ip="10.2.201.197",
                  hive_port='10000',
                  hive_user="root",
                  hive_auth="PLAIN",
                  hive_test="select * from test.test1",

                  local_path_ftp_1='D:\\DATA\\SYB\\',
                  local_path_zip_1='D:\\DATA\\SYB\\',
                  local_path_data_1='D:\\DATA\\posflow\\shouyinbao_data\\',
                  local_path_utf8_1='D:\\DATA\\posflow\\shouyinbao_utf8\\',

                  local_path_ftp_3="D:\\DATA\\posflow\\allinpay_zip_zc\\",
                  local_path_zip_3="D:\\DATA\\posflow\\allinpay_zip_zc\\",
                  local_path_data_3="D:\\DATA\\posflow\\allinpay_data_zc\\",
                  local_path_utf8_3="D:\\DATA\\posflow\\allinpay_utf8_zc\\",

                  local_path_ftp_7="D:\\DATA\\posflow\\allinpay_data_bl\\",
                  local_path_data_7="D:\\DATA\\posflow\\allinpay_data_bl\\",
                  local_path_utf8_7="D:\\DATA\\posflow\\allinpay_utf8_bl\\")

         ]

    def __init__(self, p_is_test: bool = False):
        self.m_is_test_mode = p_is_test
        self.get_sys_id()

    def get_data(self, p_name, p_default=''):
        if self.m_same_data.key_in(p_name):
            f_v = self.m_same_data.get_arg(p_name=p_name, p_default=p_default)
        elif self.m_sys_id < len(self.m_diff_data) \
                and self.m_diff_data[self.m_sys_id].key_in(p_name):
            f_v = self.m_diff_data[self.m_sys_id].get_arg(p_name=p_name, p_default=p_default)
        else:
            return p_default

        if f_v.startswith('='):
            f_v1 = f_v[1:len(f_v)]
            f_v2 = self.get_data(p_name=f_v1, p_default=p_default)
            return f_v2
        else:
            return f_v

    def is_test(self):
        return self.m_is_test_mode

    def get_sys_id(self):
        if self.m_sys_id < 0:
            if platform.platform().lower().startswith("linux") and not self.is_test():
                self.m_sys_id = 0
            if platform.platform().lower().startswith("windows") and not self.is_test():
                self.m_sys_id = 1
            if platform.platform().lower().startswith("linux") and self.is_test():
                self.m_sys_id = 2
            if platform.platform().lower().startswith("windows") and self.is_test():
                self.m_sys_id = 3
        return self.m_sys_id

    #######################
    # 1、收银宝，3、资产流水，4、资产风险、5、资产流水，6、资产风险，7、保理流水，8、保理流水，9、保理风险

    def get_zip_name(self, p_date: str = "", p_id: int = -1):
        if p_id < 0:
            p_id = self.m_project_id
        f_name0 = ""

        f_name1 = self.get_data("file_pre" + str(p_id))
        f_name2 = self.get_data("file_zip" + str(p_id))

        if len(f_name1) > 0 or len(f_name2) > 0:
            f_name0 = f_name1 + p_date + f_name2

        return f_name0

    def get_ftp_name(self, p_date: str = "", p_id: int = -1):
        if p_id < 0:
            p_id = self.m_project_id
        f_name0 = ""

        f_name1 = self.get_data("file_pre" + str(p_id))
        f_name2 = self.get_data("file_ftp" + str(p_id))

        if len(f_name1) > 0 or len(f_name2) > 0:
            f_name0 = f_name1 + p_date + f_name2

        return f_name0

    def get_file_name(self, p_date: str = "", p_id: int = -1):
        if p_id < 0:
            p_id = self.m_project_id
        f_name0 = ""

        f_name1 = self.get_data("file_pre" + str(p_id))
        f_name2 = self.get_data("file_ext" + str(p_id))

        if len(f_name1) > 0 or len(f_name2) > 0:
                f_name0 = f_name1 + p_date + f_name2

        return f_name0

    def get_table_name(self, p_id: int = -1):
        if p_id < 0:
            p_id = self.m_project_id
        return self.get_data("hive_table" + str(p_id))

    def get_hive_head(self, p_id: int = -1):
        # hive_head1 = "1",  # 1、有表头，需要删除, need_head=False
        # hive_head3 = "0",  # 0、无表头，不要删除, need_head=True
        f_return_value = True
        if p_id < 0:
            p_id = self.m_project_id
        f_value = self.get_data("hive_head" + str(p_id))
        if len(f_value) > 0 and f_value.isdecimal():
            if int(f_value) > 0:
                f_return_value = False
        return f_return_value

    def get_hive_add_date(self, p_date: str = "", p_id: int = -1):
        # hive_add_date_10 = "1",  # 1、要增加日期字段
        f_return_value = ""
        if p_id < 0:
            p_id = self.m_project_id
        f_v = self.get_data("hive_add_date_" + str(p_id))
        if len(f_v) > 0 and f_v.isdecimal():
            if int(f_v) > 0:
                f_return_value = p_date
        return f_return_value

    def get_ftp_ip(self, p_id: int = -1):
        if p_id < 0:
            p_id = self.m_project_id
        f_v = self.get_data("ftp_ip_" + str(p_id))
        return f_v

    def get_ftp_port(self, p_id: int = -1):
        if p_id < 0:
            p_id = self.m_project_id
        f_v = self.get_data("ftp_port_" + str(p_id))
        return f_v

    def get_ftp_user(self, p_id: int = -1):
        if p_id < 0:
            p_id = self.m_project_id
        f_v = self.get_data("ftp_user_" + str(p_id))
        return f_v

    def get_ftp_pass(self, p_id: int = -1):
        if p_id < 0:
            p_id = self.m_project_id
        f_v = self.get_data("ftp_pass_" + str(p_id))
        return f_v

    def get_remote_path_ftp(self, p_date: str = "", p_id: int = -1):
        if p_id < 0:
            p_id = self.m_project_id
        f_v1 = self.get_data("remote_path_ftp_" + str(p_id))
        f_v2 = self.get_data("remote_date_ftp_" + str(p_id))
        if len(f_v2) > 0:
            f_v1 = str(pathlib.PurePosixPath(f_v1).joinpath(p_date))
        return f_v1

    def get_hdfs_path(self, p_id: int = -1):
        if p_id < 0:
            p_id = self.m_project_id
        f_v = self.get_data("hdfs_dir_" + str(p_id))
        return f_v

    #######################

    def get_local_path_ftp(self, p_id: int = -1):
        if p_id < 0:
            p_id = self.m_project_id
        f_v = self.get_data("local_path_ftp_" + str(p_id))
        return f_v

    def get_zip_path(self, p_id: int = -1):
        if p_id < 0:
            p_id = self.m_project_id
        f_v = self.get_data("local_path_zip_" + str(p_id))
        return f_v

    def get_data_path(self, p_id: int = -1):
        if p_id < 0:
            p_id = self.m_project_id
        f_v = self.get_data("local_path_data_" + str(p_id))
        return f_v

    def get_utf8_path(self, p_id: int = -1):
        if p_id < 0:
            p_id = self.m_project_id
        f_v = self.get_data("local_path_utf8_" + str(p_id))
        return f_v

    #######################

    def test_date(self, isBaoli=True):
        return self.get_data("test_date")

    def hdfs_ip(self, isBaoli=True):
        return "http://" + self.get_data("cdh_ip") + ":" + self.get_data("hdfs_port")

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

