#!coding:utf-8

import platform


class conf_data:

    @staticmethod
    def isTest():
        istest = False
        istest = True
        return istest

    @staticmethod
    def root_path(isBaoli=True):
        if platform.platform().lower().startswith("linux"):
            root_path = "/home/data/SYB/"

        if conf_data.isTest():
            root_path = "/media/bd/data/toshiba/data/SYB/"

        if platform.platform().lower().startswith("windows"):
            root_path = 'D:/DATA/SYB/'

        return root_path

    @staticmethod
    def unzip_dir(isBaoli=True):
        if platform.platform().lower().startswith("linux"):
            destdir = "/home/data/posflow/shouyinbao/UNZIP/"

        if conf_data.isTest():
            if isBaoli:
                destdir = "/home/bd/桌面/201811_flow/bl_shouyinbao/UNZIP/"
            else:
                destdir = "/home/bd/桌面/201811_flow/zc_shouyinbao/UNZIP/"

        if platform.platform().lower().startswith("windows"):
            destdir = 'D:/DATA/UNZIP/'

        return destdir

    @staticmethod
    def decode_dir(isBaoli=True):
        if platform.platform().lower().startswith("linux"):
            destdir = "/home/data/posflow/shouyinbao/UTF8/"

        if conf_data.isTest():
            if isBaoli:
                destdir = "/home/bd/桌面/201811_flow/bl_shouyinbao/UTF8/"
            else:
                destdir = "/home/bd/桌面/201811_flow/zc_shouyinbao/UTF8/"

        if platform.platform().lower().startswith("windows"):
            destdir = 'D:/DATA/UTF8/'

        return destdir

    @staticmethod
    def hdfs_dir(isBaoli=True):
        if platform.platform().lower().startswith("linux"):
            destdir = "/data/posflow/shouyinbao/UTF8/"

        if conf_data.isTest():
            if isBaoli:
                destdir = "/shouyinbao/bl_shouyinbao/UTF8/"
            else:
                destdir = "/shouyinbao/zc_shouyinbao/UTF8/"

        if platform.platform().lower().startswith("windows"):
            destdir = 'D:/DATA/UTF8/'

        return destdir

    @staticmethod
    def filepre1(isBaoli=True):
        filepre = 't1_trxrecord_'
        return filepre

    @staticmethod
    def fileext1(isBaoli=True):
        fileext = "_V2.zip"
        return fileext

    @staticmethod
    def fileext2(isBaoli=True):
        fileext = "_V2.csv"
        return fileext

    @staticmethod
    def testdate(isBaoli=True):
        thedate = "20181101"
        return thedate

    @staticmethod
    def hdfs_IP(isBaoli=True):
        host = "http://10.91.1.20:50070"
        if conf_data.isTest():
            host = "http://10.2.201.197:50070"
        return host

    @staticmethod
    def hive_IP():
        host = "10.91.1.20"
        if conf_data.isTest():
            host = "10.2.201.197"
        return host

    @staticmethod
    def hive_port():
        port = 10000
        return port

    @staticmethod
    def hive_user():
        user = "root"
        if conf_data.isTest():
            user = "root"
        return user

    @staticmethod
    def hive_auth():
        auth = "PLAIN"
        return auth

    @staticmethod
    def hive_test():
        test = "select * from rds.area"

        if conf_data.isTest():
            test = "select * from test.test1"
        return test

    @staticmethod
    def hive_table(isBaoli=True):
        tbl = "posflow.t1_trxrecprd_V2"

        if conf_data.isTest():
            if isBaoli:
                tbl = 'test.t1_trxrecprd_v2_bl'
            else:
                tbl = 'test.t1_trxrecprd_v2_zc'

        return tbl
