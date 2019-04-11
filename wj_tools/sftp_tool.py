# -*- coding: utf-8 -*-

# from wj_tools import sftp_config
from wj_tools import sftpUtil
from wj_tools.mylog import myLog
from wj_tools.file_check import MyLocalFile
from wj_tools.str_tool import StrTool
import paramiko
import os,stat,shutil
import time,datetime


class Sftp_Tool:
    # SFTP服务器的IP、端口、账户、密码
    __default_host = "172.31.71.71"           # type: str
    __default_port = 12306                    # type: int
    __default_username = "yuanxj"             # type: str
    __default_password = "Uwj1qsFnV8"         # type: str
    # remote和local是相对客户端的
    __default_remoteDir = "/tmp/"             # type: str
    __default_localDir = "/home/data/thzc/"   # type: str
    # test on Windows
    # __default_localDir = "D:/sftp/"
    ##########################

    # SFTP服务器的IP、端口、账户、密码
    __m_host = __default_host                     # type: str
    __m_port = __default_port                     # type: str
    __m_username = __default_username             # type: str
    __m_password = __default_password             # type: str
    # remote和local是相对客户端的
    __m_remoteDir = __default_remoteDir           # type: str
    __m_localDir = __default_localDir             # type: str
    __theSftp = None                              # type: paramiko.SFTPClient   # 不是 Transport  # Ftp连接

    def __init__(self, h='', p=0, u='', s='', r='', d=''):
        # __m_host
        if type(h) is not str or len(h) == 0:
            self.__m_host = self.__default_host
        else:
            self.__m_host = h
        # __m_port
        if p == 0:
            self.__m_port = self.__default_port
        else:
            self.__m_port = p
        # __m_username
        if type(u) is not str or len(u) == 0:
            self.__m_username = self.__default_username
        else:
            self.__m_username = u
        # __m_password
        if type(s) is not str or len(s) == 0:
            self.__m_password = self.__default_password
        else:
            self.__m_password = s
        # __m_remoteDir
        if type(r) is not str or len(r) == 0:
            self.__m_remoteDir = self.__default_remoteDir
        else:
            self.__m_remoteDir = r
        #
        if type(d) is not str or len(d) == 0:
            self.__m_localDir = self.__default_localDir
        else:
            self.__m_localDir = d

    def openSFTP(self):
        self.closeSFTP()
        result = sftpUtil.getConnect(self.__m_host, self.__m_port, self.__m_username, self.__m_password)
        if result[0] == 1:
            self.__theSftp = paramiko.SFTPClient.from_transport(result[2])
        else:
            self.__theSftp = None
            myLog.Log('sftp 连接失败', False)

    def closeSFTP(self):
        ###  需要调整!!!!!!!!!!!!!!!!!!!!!!!!!
        if (self.__theSftp is not None) and isinstance(self.__theSftp, paramiko.SFTPClient):
            try:
                self.__theSftp.close()
                myLog.Log('sftp 关闭')
            except Exception as e2:
                myLog.Log("出错: " + str(e2), False)
        self.__theSftp = None

    def __checkStartEndTime(self, mtime: float, sdate: str='', edate: str='', default: bool=True):
        """确定开始结束时间.

        # a. 只有开始时间，就取 sdate 的整日
        # b. 只有结束时间，就取 1970 到 edate
        # c. 同时有，就取之间
        # d . 都没有，就取 19700501 - 21000101
        if (edate is None) or len(edate) <= 0:  # 没有结束时间


        :param sdate: str
        :param edate: str
        :return: s_date，e_date
        """
        if type(edate) is not str or len(edate) <= 0:  # 没有结束时间
            if type(sdate) is not str or len(sdate) <= 0:  # d. 没有开始时间， 没有结束时间
                s_date = StrTool.get_the_date_tick("19700501")
                e_date = StrTool.get_the_date_tick("21000101")
                return default, True
            else:  # a. 有开始时间， 没有结束时间
                s_date = StrTool.get_the_date_tick(sdate)
                e_date = StrTool.get_the_date_tick("21000101")
        else:  # w xian
            if type(sdate) is not str or len(sdate) <= 0:  # b. 没有开始时间，有结束时间
                s_date = StrTool.get_the_date_tick("19700501")
                e_date = time.mktime((StrTool.get_the_date(edate) + datetime.timedelta(days=1)).timetuple())
            else:  # c. 有开始时间，有结束时间
                s_date = StrTool.get_the_date_tick(sdate)
                e_date = time.mktime((StrTool.get_the_date(edate) + datetime.timedelta(days=1)).timetuple())

        if s_date <= mtime < e_date:
            return True, False
        else:
            return False, False

        # return s_date,e_date

    def __checkFileInList(self, name: str, fileNames: list=[], default: bool=True):
        """检查文件名是否在列表中.


                    # p =f.filename.rfind('.')
                    # if ( p < 0 ):
                    #     t_ext = ''
                    #     t_name = f.filename
                    # else:
                    #     t_ext= f.filename[p+1:len(f.filename)]
                    #     t_name = f.filename[0:p]
                    # ^ 获取文件名和后缀

        :param name:
        :param fileNames: list
        :return:
        """
        # 包括name is None
        if type(name) is not str or len(name) <= 0:
            return default, True

        if type(fileNames) is str:
            fileNames = fileNames.split(',')

        if type(fileNames) is not list:
            return default, True

        num = 0
        for data in fileNames:
            if type(data) is str and len(data) > 0:
                num += 1

        if num == 0:
            return default, True

        if name in fileNames:
            return True, False
        else:
            return False, False

    def getRmFilesList(self, rmdir: str, p_name: str= '', sdate: str= '', edate: str= '',
                       fileNames: list=[], and_op: bool=True):
        """###  需要调整!!!!!!!!!!!!!!!!!!!!!!!!!

        # s_date, e_date = self.__getStartEndTime(sdate, edate)
        # and 的时候，不给fileNames列表，__checkFileInList = True
        # and 的时候，不给开始结束时间，__getStartEndTime = True
        # and 的时候，不给开始结束字符，checkname = True

        # checkname and __getStartEndTime and __checkFileInList

        # or 的时候，不给fileNames列表，__checkFileInList = False
        # or 的时候，不给开始结束时间，__getStartEndTime = False
        # or 的时候，不给开始结束字符，checkname = False

        # checkname or __getStartEndTime or __checkFileInList or
        # ( not checkname and not __getStartEndTime or not __checkFileInList )

        :param rmdir:
        :param p_name:
        :param sdate:
        :param edate:
        :param fileNames:
        :param and_op:
        :return:
        """
        retFiles = []   # type: list[paramiko.SFTPAttributes]
        if type(rmdir) is not str or len(rmdir) == 0:
            rmdir = self.__m_remoteDir
        if self.__theSftp is None or (not isinstance(self.__theSftp, paramiko.SFTPClient)):
            return retFiles
        try:
            rmdir = rmdir.replace('\\', '/')
            listFiles = self.__theSftp.listdir_attr(rmdir)
            if len(listFiles) > 0:
                for f in listFiles:
                    if not stat.S_ISREG(f.st_mode):
                        continue
                    shortname = f.filename
                    v1, d1 = self.__checkFileInList(shortname, fileNames, default=and_op)
                    v2, d2 = self.__checkStartEndTime(f.st_mtime, sdate, edate, default=and_op)
                    v3, d3 = MyLocalFile.check_name(shortname, p_name, default=and_op)
                    if and_op:
                        if v1 and v2 and v3:
                            retFiles.append(f)
                    else:
                        if v1 or v2 or v3 or (d1 and d2 and d3):
                            retFiles.append(f)
        except Exception as e:
            myLog.Log('sftp 文件列表失败:' + str(e), False)
            # traceback.print_exc()
        return retFiles


    def getLcFilesList(self, localdir: str, p_name: str= '', sdate: str= '', edate: str= '',
                       fileNames: list=[], and_op: bool=True):
        """###  需要调整!!!!!!!!!!!!!!!!!!!!!!!!!

        # s_date, e_date = self.__getStartEndTime(sdate, edate)
        # and 的时候，不给fileNames列表，__checkFileInList = True
        # and 的时候，不给开始结束时间，__getStartEndTime = True
        # and 的时候，不给开始结束字符，checkname = True

        # checkname and __getStartEndTime and __checkFileInList

        # or 的时候，不给fileNames列表，__checkFileInList = False
        # or 的时候，不给开始结束时间，__getStartEndTime = False
        # or 的时候，不给开始结束字符，checkname = False

        # checkname or __getStartEndTime or __checkFileInList or
        # ( not checkname and not __getStartEndTime or not __checkFileInList )

        :param localdir:
        :param p_name:
        :param sdate:
        :param edate:
        :param fileNames:
        :param and_op:
        :return:
        """
        retFiles = []   # type: list
        if type(localdir) is not str or len(localdir) == 0:
            localdir = self.__m_localDir
        try:
            listFiles = MyLocalFile.get_child(localdir)
            if len(listFiles) > 0:
                for f in listFiles:
                    st = os.stat(f)
                    if not stat.S_ISREG(st.st_mode):
                        continue
                    shortname = os.path.basename(f)
                    v1, d1 = self.__checkFileInList(shortname, fileNames, default=and_op)
                    v2, d2 = self.__checkStartEndTime(st.st_mtime, sdate, edate, default=and_op)
                    v3, d3 = MyLocalFile.check_name(shortname, p_name, default=and_op)
                    if and_op:
                        if v1 and v2 and v3:
                            retFiles.append(f)
                    else:
                        if v1 or v2 or v3 or (d1 and d2 and d3):
                            retFiles.append(f)
        except Exception as e:
            myLog.Log('local 文件列表失败:' + str(e), False)
            # traceback.print_exc()
        return retFiles

    def download_files(self, from_dir='', to_dir='', p_name: str= '',
                       sdate: str= '', edate: str= '', file_names: list=[], and_op: bool=True):
        # 设置默认值
        if not isinstance(self.__theSftp, paramiko.SFTPClient):
            myLog.error('文件下载失败，没有下载FTP类')
            return
        if type(from_dir) is not str or len(from_dir) <= 0:
            from_dir = self.__m_remoteDir
        if type(to_dir) is not str or len(to_dir) <= 0:
            to_dir = self.__m_localDir
        myLog.Log('文件下载开始 from:' + from_dir + ' to: ' + to_dir)
        # 设置默认值

        fileList = self.getRmFilesList(from_dir, p_name, sdate, edate, file_names, and_op)  # 只处理列表中的文件

        for fromFile in fileList:
            shortname = fromFile.filename   # fromFile.filename        # fromFile.st_atime        # fromFile.st_mtime
            toFile = os.path.join(to_dir, shortname)

            isdownloaded = False
            srcFile = os.path.join(from_dir, shortname)
            # stinfo1 = theSftp.stat(srcFile)
            # 可以用上面的 fromFile 里的信息代替
            # - ``st_size``
            # - ``st_uid``
            # - ``st_gid``
            # - ``st_mode``
            # - ``st_atime``
            # - ``st_mtime``

            if os.path.isfile(toFile):  # 文件已经存在，就比对下大小、时间
                stinfo2 = os.stat(toFile)
                if (fromFile.st_size == stinfo2.st_size and abs(
                        int(fromFile.st_mtime - stinfo2.st_mtime)) < 10):  # 本地文件时间是float
                    isdownloaded = True

            if not isdownloaded:
                MyLocalFile.safe_make_dir(to_dir, stat.S_IRWXO + stat.S_IRWXG + stat.S_IRWXU)

                try:
                    srcFile = srcFile.replace('\\', '/')
                    self.__theSftp.get(srcFile, toFile)
                    # 修改访问和修改时间
                    os.chmod(toFile,
                             stat.S_IWOTH + stat.S_IROTH + stat.S_IWGRP + stat.S_IRGRP + stat.S_IWUSR + stat.S_IRUSR)
                    os.utime(toFile,
                             (fromFile.st_atime, fromFile.st_mtime))
                    myLog.Log('成功下载 ' + toFile)
                except Exception as e:
                    myLog.error(str(e))
                    myLog.Log('文件下载失败：' + toFile)
            else:
                myLog.Log('已经存在 ' + toFile)

        myLog.Log('文件下载结束 from:' + from_dir + ' to: ' + to_dir)

    def copy_files(self, fromDir='', toDir='', p_name: str= '',
                   sdate: str= '', edate: str= '', fileNames: list=[], and_op: bool=True):
        # 设置默认值
        if type(fromDir) is not str or len(fromDir) <= 0:
            fromDir = self.__m_localDir
        if type(toDir) is not str or len(toDir) <= 0:
            myLog.error('纯文件复制失败，没有复制目的地址')
            return
        myLog.Log('纯文件复制开始 from:' + fromDir + ' to: ' + toDir)
        # 设置默认值

        fileList = self.getLcFilesList(fromDir, p_name, sdate, edate, fileNames, and_op)  # 只处理列表中的文件

        for fromFile in fileList:
            if os.path.isfile(fromFile):
                stinfo1 = os.stat(fromFile)
            else:
                myLog.Log('文件不存在 ' + fromFile)
                continue

            shortname = os.path.basename(fromFile)
            toFile = os.path.join(toDir, shortname)

            isdownloaded = False

            if os.path.isfile(toFile):
                stinfo2 = os.stat(toFile)
                if (stinfo1.st_size == stinfo2.st_size and abs(
                        stinfo2.st_mtime - stinfo1.st_mtime) < 10):  # 本地文件时间是float
                    isdownloaded = True

            if not isdownloaded:
                MyLocalFile.safe_make_dir(toDir, stat.S_IRWXO + stat.S_IRWXG + stat.S_IRWXU)
                try:
                    shutil.copyfile(fromFile, toFile)
                    # 修改访问和修改时间
                    os.chmod(toFile,
                             stat.S_IWOTH + stat.S_IROTH + stat.S_IWGRP + stat.S_IRGRP + stat.S_IWUSR + stat.S_IRUSR)
                    os.utime(toFile,
                             (stinfo1.st_atime, stinfo1.st_mtime))
                    myLog.Log('成功Copy文件 ' + toFile)
                except Exception as e:
                    myLog.error(str(e))
                    myLog.Log('文件Copy失败：' + toFile)
            else:
                myLog.Log('已经存在 ' + toFile)

        myLog.Log('纯文件复制结束 from:' + fromDir + ' to: ' + toDir)
