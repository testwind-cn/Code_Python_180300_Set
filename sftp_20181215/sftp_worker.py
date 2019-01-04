# -*- coding: utf-8 -*-
from wj_tools.mylog import myLog
from wj_tools import sftp_tool
import os, stat
import time
import datetime
import dataClean
from wj_tools.file_check import MyLocalFile
from wj_tools.str_tool import StrTool


class Sftp_Worker:
    # SFTP服务器的IP、端口、账户、密码
    __m_host = "172.31.71.71"           # type: str
    __m_port = 12306                    # type: int
    __m_username = "yuanxj"             # type: str
    __m_password = "Uwj1qsFnV8"         # type: str
    # remote和local是相对客户端的
    __m_remoteDir = "/tmp/"             # type: str
    __m_localDir = "/home/data/thzc/"   # type: str
    # test on Windows
    # __default_localDir = "D:/sftp/"
    ##########################

    # SFTP服务器的IP、端口、账户、密码
    host = __m_host                     # type: str
    port = __m_port                     # type: str
    username = __m_username             # type: str
    password = __m_password             # type: str
    # remote和local是相对客户端的
    remoteDir = __m_remoteDir           # type: str
    localDir = __m_localDir             # type: str
    __theSftp = None                      # type: sftp_tool.Sftp_Tool # 不是 Transport  # Ftp连接

    def __init__(self, h='', p=0, u='', s='', r='', d=''):
        # __m_host
        if type(h) is not str or len(h) == 0:
            self.host = self.__m_host
        else:
            self.host = h
        # __m_port
        if p == 0:
            self.port = self.__m_port
        else:
            self.port = p
        # __m_username
        if type(u) is not str or len(u) == 0:
            self.username = self.__m_username
        else:
            self.username = u
        # __m_password
        if type(s) is not str or len(s) == 0:
            self.password = self.__m_password
        else:
            self.password = s
        # __m_remoteDir
        if type(r) is not str or len(r) == 0:
            self.remoteDir = self.__m_remoteDir
        else:
            self.remoteDir = r
        #
        if type(d) is not str or len(d) == 0:
            self.localDir = self.__m_localDir
        else:
            self.localDir = d

        self.__theSftp = sftp_tool.Sftp_Tool(h, p, u, s, r, d)

    def openSFTP(self):
        if isinstance(self.__theSftp, sftp_tool.Sftp_Tool):
            self.__theSftp.openSFTP()
        else:
            myLog.Log('sftp 连接失败', False)

    def closeSFTP(self):
        if isinstance(self.__theSftp, sftp_tool.Sftp_Tool):
            self.__theSftp.closeSFTP()
        else:
            self.__theSftp = None

    def downloadFilesByDay(self, to_dir='', day_str='', file_names=None, from_dir=''):
        # 设置默认值
        if self.__theSftp is None:
            return
        if type(from_dir) is not str or len(from_dir) == 0:
            from_dir = self.remoteDir
        if type(to_dir) is not str or len(to_dir) == 0:
            to_dir = self.localDir
        day_str = StrTool.get_the_date_str(day_str)
        myLog.Log('单日文件下载开始' + day_str)
        # 设置默认值

        self.__theSftp.download_files(from_dir=from_dir, to_dir=os.path.join(to_dir, day_str),
                                      start='', ext='', fstr=day_str,
                                      sdate=day_str, edate=day_str,
                                      file_names=file_names, and_op=False)

        myLog.Log('单日文件下载结束 ' + day_str)

    def downloadFilesByRange(self, day_str: str= '', days: int=1, from_dir: str= '', to_dir: str= ''):
        # 设置默认值
        if self.__theSftp is None:
            return
        if type(from_dir) is not str or len(from_dir) == 0:
            from_dir = self.remoteDir
        if type(to_dir) is not str or len(to_dir) == 0:
            to_dir = self.localDir
        day_str = StrTool.get_the_date_str(day_str)
        myLog.Log('批量下载文件开始')
        date1 = StrTool.get_the_date(day_str)
        # 设置默认值

        for i in range(0, days):
            date2 = date1 - datetime.timedelta(days=(days - i - 1))
            day_str2 = date2.strftime("%Y%m%d")
            self.__theSftp.download_files(from_dir=from_dir, to_dir=os.path.join(to_dir, day_str2),
                                          start='', ext='', fstr=day_str2,
                                          sdate=day_str2, edate=day_str2,
                                          file_names=[], and_op=False)

        myLog.Log('批量下载文件结束')

    def copyFilesByDay(self, to_dir: str, day_str: str='', file_names: list=[], from_dir: str= ''):
        # 检查默认值
        if type(to_dir) is not str or len(to_dir) == 0:
            return
        if type(from_dir) is not str or len(from_dir) == 0:
            from_dir = self.localDir
        day_str = StrTool.get_the_date_str(day_str)
        myLog.Log('单日文件复制开始 ' + day_str)
        # 检查默认值

        searchFileNames = []
        if isinstance(file_names, list):
            for fileName in file_names:
                searchFileNames.append(fileName + day_str + '.csv')

        self.__theSftp.copy_files(fromDir=os.path.join(from_dir, day_str), toDir=to_dir, start='', ext='', fstr='',
                                  sdate='', edate='', fileNames=searchFileNames, and_op=False)

    def copyFilesByRange(self, to_dir: str, day_str: str='', days: int=1, file_names: list=[], from_dir: str= ''):
        # 检查默认值
        if type(to_dir) is not str or len(to_dir) == 0:
            return
        if type(from_dir) is not str or len(from_dir) == 0:
            from_dir = self.localDir
        day_str = StrTool.get_the_date_str(day_str)
        date1 = StrTool.get_the_date(day_str)
        # 检查默认值

        myLog.Log('批量复制文件开始')

        for i in range(0, days):
            date2 = date1 - datetime.timedelta(days=(days - i - 1))
            thedayStr2 = date2.strftime("%Y%m%d")
            self.copyFilesByDay(to_dir=to_dir, day_str=thedayStr2, file_names=file_names, from_dir=from_dir)

        myLog.Log('批量复制文件结束')

    def cleanFilesByDay(self, day_str: str=''):
        day_str = StrTool.get_the_date_str(day_str)
        thedate = StrTool.get_the_date(day_str)
        myLog.Log(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())) + ' 文件清洗开始:clean、rename、append ' + day_str)

        dataClean.dataCleanTrustApply(day_str)
        dataClean.renameFiles(day_str)
        dataClean.appendData(thedate, day_str)

        myLog.Log(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())) + ' 文件清洗结束 ' + day_str)

    def cleanFilesByRange(self, day_str: str='', days: int=1):
        day_str = StrTool.get_the_date_str(day_str)
        date1 = StrTool.get_the_date(day_str)

        myLog.Log('批量文件清洗开始')

        for i in range(0, days):
            date2 = date1 - datetime.timedelta(days=(days - i - 1))
            thedayStr2 = date2.strftime("%Y%m%d")
            self.cleanFilesByDay(day_str=thedayStr2)

        myLog.Log('批量文件清洗结束')

    def cleanFilePermission(self):
        fileList = MyLocalFile.get_child(dataClean.aimPath)
        for aFile in fileList:
            os.chmod(aFile, stat.S_IWOTH + stat.S_IROTH + stat.S_IWGRP + stat.S_IRGRP + stat.S_IWUSR + stat.S_IRUSR)


    def removeFiles(self,deleteDay:str):
        if fileName.find(deleteDay) >= 0:
            myLog.Log('预删除 ' + fileName)
            sftp.remove(sftp_config.homeDir + fileName)
            logging.info('成功删除 ' + fileName)

            if self.__theSftp is None:
                return
            if type(from_dir) is not str or len(from_dir) == 0:
                from_dir = self.remoteDir
            if type(to_dir) is not str or len(to_dir) == 0:
                to_dir = self.localDir
            day_str = StrTool.get_the_date_str(day_str)
            myLog.Log('批量下载文件开始')
            date1 = StrTool.get_the_date(day_str)

