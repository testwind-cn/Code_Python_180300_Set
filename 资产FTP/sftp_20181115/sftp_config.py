# -*- coding: utf-8 -*-
import sys
import logging
import traceback
import os,stat
import shutil
import time
import paramiko
import datetime
import sftpUtil
import dataClean
from file_check import myLocalFile



class Sftp_Tool:
    # SFTP服务器的IP、端口、账户、密码
    __m_host = "172.31.71.71"
    __m_port = 12306  # not str
    __m_username = "yuanxj"
    __m_password = "Uwj1qsFnV8"
    # remote和local是相对客户端的
    __m_remoteDir = "/tmp/"
    __m_localDir = "/home/data/thzc/"
    # test on Windows
    __m_localDir = "D:/sftp/"
    ##########################


    # SFTP服务器的IP、端口、账户、密码
    host = __m_host
    port = __m_port
    username = __m_username
    password = __m_password
    # remote和local是相对客户端的
    remoteDir = __m_remoteDir
    localDir = __m_localDir
    theSftp = None  # Ftp连接

    def __init__(self, h='', p='', u='', s='',r='', l=''):
        # host
        if h is None or len(h) == 0:
            self.host = self.__m_host
        else:
            self.host = h
        # port
        if p is None or len(p) == 0:
            self.port = self.__m_port
        else:
            self.port = p
        # username
        if u is None or len(u) == 0:
            self.username = self.__m_username
        else:
            self.username = u
        # password
        if s is None or len(s) == 0:
            self.password = self.__m_password
        else:
            self.password = s
        # remoteDir
        if r is None or len(r) == 0:
            self.remoteDir = self.__m_remoteDir
        else:
            self.remoteDir = r
        #
        if l is None or len(l) == 0:
            self.localDir = self.__m_localDir
        else:
            self.localDir = l

    def sftpLog(self,info='',isInfo=True):
        if isInfo:
            logging.info(info)
        else:
            logging.error(info)
        print(info)

    def getTheDateStr(self,thedayStr=''):
        try:
            stime = time.strptime(thedayStr, "%Y%m%d")
            thedayStr = time.strftime("%Y%m%d", stime)
        except Exception as et:
            theday = datetime.date.today()
            thedayStr = theday.strftime("%Y%m%d")
        #        curYear = 2018
        #        theday = theday.replace(curYear, theday.month, theday.day)
        #        thedayStr = str(theday).replace('-', '')

        return thedayStr

    def getTheDate(self,thedayStr=''):
        #    thedayStr = getTheDateStr(thedayStr)
        try:
            date1 = datetime.datetime.strptime(thedayStr, "%Y%m%d").date()
        except Exception as e:
            date1 = datetime.date.today()
        return date1

    def getTheDateTick(self,thedayStr=''):
        #    thedayStr = getTheDateStr(thedayStr)
        try:
            stime = time.strptime(thedayStr, "%Y%m%d")
            thedatetick = time.mktime(stime)
        except Exception as e:
            date1 = datetime.date.today()
            thedatetick = time.mktime(date1.timetuple())
        return thedatetick

    def safeMakedir(self,dirStr):
        try:
            if not os.path.exists(dirStr):
                os.makedirs(dirStr)
                os.chmod(dirStr, stat.S_IRWXO + stat.S_IRWXG + stat.S_IRWXU)
        except Exception as e:
            self.sftpLog("safeMakedir Error: "+str(e), False)
        return

    def openSFTP(self):
        self.closeSFTP()
        result = sftpUtil.getConnect(self.host, self.port, self.username, self.password)
        if result[0] == 1:
            self.theSftp = paramiko.SFTPClient.from_transport(result[2])
        else:
            self.sftpLog('sftp 连接失败', False)

    def closeSFTP(self):
        ###  需要调整!!!!!!!!!!!!!!!!!!!!!!!!!
        if (not self.theSftp is None) and isinstance(self.theSftp, paramiko.SFTPClient):
            try:
                self.theSftp.close()
                self.sftpLog('sftp 关闭')
            except Exception as e2:
                self.sftpLog("出错: "+str(e2), False)
        self.theSftp = None

    def getFilesList(self, rmdir, start='', ext='', sdate='', edate=''):
        ###  需要调整!!!!!!!!!!!!!!!!!!!!!!!!!
        retFiles = []
        # a. 只有开始时间，就取 sdate 的整日
        # b. 只有结束时间，就取 1970 到 edate
        # c. 同时有，就取之间
        # d . 都没有，就取 1970 - 2100
        if (edate is None) or len(edate) <= 0:      #没有结束时间
            if (sdate is None) or len(sdate) <= 0:  # d. 没有开始时间
                s_date = self.getTheDateTick("19700101")
                e_date = self.getTheDateTick("21000101")
            else:                                   # a. 有开始时间
                s_date = self.getTheDateTick(sdate)
                e_date = time.mktime((self.getTheDate(sdate) + datetime.timedelta(days=1)).timetuple())
        else:                                       # 有结束时间
            if (sdate is None) or len(sdate) <= 0:  # b. 没有开始时间
                s_date = self.getTheDateTick("19700101")
                e_date = time.mktime((self.getTheDate(edate) + datetime.timedelta(days=1)).timetuple())
            else:                                   # c. 有开始时间
                s_date = self.getTheDateTick(sdate)
                e_date = time.mktime((self.getTheDate(edate) + datetime.timedelta(days=1)).timetuple())

        if self.theSftp is None or (not isinstance(self.theSftp, paramiko.SFTPClient)):
            return retFiles
        try:
            listFiles = self.theSftp.listdir_attr(rmdir)
            if len(listFiles) > 0:
                for f in listFiles:
                    # p =f.filename.rfind('.')
                    # if ( p < 0 ):
                    #     t_ext = ''
                    #     t_name = f.filename
                    # else:
                    #     t_ext= f.filename[p+1:len(f.filename)]
                    #     t_name = f.filename[0:p]
                    # ^ 获取文件名和后缀
                    if (len(start) == 0 or f.filename.lower().startswith(start.lower())) and \
                            (len(ext) == 0 or f.filename.lower().endswith(ext.lower())) and \
                            s_date <= f.st_mtime and f.st_mtime < e_date:
                        retFiles.append(f)
        except Exception as e:
            self.sftpLog('sftp 文件列表失败:'+str(e), False)
            # traceback.print_exc()
        return retFiles

    def downloadFiles(self, theSftp='default', thedayStr='', allFiles=None, fromRemoteDir='', toLocalDir=''):
        return

    def downloadFilesByDay(self, theSftp='default', thedayStr='', allFiles=None, fromRemoteDir='', toLocalDir=''):
        if theSftp == 'default' or theSftp is None:
            return

        self.theSftp = theSftp

        if fromRemoteDir is None or len(fromRemoteDir) == 0:
            fromRemoteDir = self.remoteDir

        if toLocalDir is None or len(toLocalDir) == 0:
            toLocalDir = self.localDir

        getList = self.getFilesList(fromRemoteDir, start='', ext='', sdate=thedayStr, edate='')

        self.sftpLog('单日文件下载开始' + thedayStr)

        self.safeMakedir(toLocalDir)

        for aFile in getList:
            # aFile.filenmae        # aFile.st_atime        # aFile.st_mtime
            realFileName = aFile.filename
            if allFiles is None or len(allFiles) == 0 or realFileName in allFiles:
                self.safeMakedir(os.path.join(toLocalDir, thedayStr))
                try:
                    isdownloaded = False
                    srcFile = os.path.join(fromRemoteDir, realFileName)
                    destFile = os.path.join(toLocalDir, thedayStr + '/', realFileName)
                    stinfo1 = theSftp.stat(srcFile)
                    # 可以用上面的 aFile 里的信息代替

                    if os.path.isfile(destFile): # 文件已经存在，就比对下大小、时间
                        stinfo2 = os.stat(destFile)
                        if (stinfo1.st_size == stinfo2.st_size and abs(
                                int(stinfo2.st_mtime) - stinfo1.st_mtime) < 10):  # 本地文件时间是float
                            isdownloaded = True

                    if isdownloaded == False:
                        theSftp.get(srcFile, destFile)
                        # 修改访问和修改时间
                        os.chmod(destFile,
                                 stat.S_IWOTH + stat.S_IROTH + stat.S_IWGRP + stat.S_IRGRP + stat.S_IWUSR + stat.S_IRUSR)
                        os.utime(destFile,
                                 (stinfo1.st_atime, stinfo1.st_mtime))
                        self.sftpLog('成功下载 ' + destFile)
                    else:
                        self.sftpLog('已经存在 ' + destFile)

                except Exception as e:
                    traceback.print_exc()
                    logging.error(str(e))
                    self.sftpLog('文件下载失败：' + destFile)

        self.sftpLog('单日文件下载结束 ' + thedayStr)

    def downloadFilesByRange(self, thedayStr='', days=1, fromRemoteDir='', toLocalDir=''):
        theSftp = 'default'

        if fromRemoteDir is None or len(fromRemoteDir) == 0:
            fromRemoteDir = self.remoteDir

        if toLocalDir is None or len(toLocalDir) == 0:
            toLocalDir = self.localDir

        thedayStr1 = self.getTheDateStr(thedayStr)
        date1 = self.getTheDate(thedayStr1)

        self.sftpLog('批量下载文件开始')

        result = sftpUtil.getConnect(self.host, self.port, self.username, self.password)

        if result[0] == 1:
            try:
                theSftp = paramiko.SFTPClient.from_transport(result[2])
                for i in range(0, days):
                    date2 = date1 - datetime.timedelta(days=(days - i - 1))
                    thedayStr2 = date2.strftime("%Y%m%d")
                    self.downloadFilesByDay(theSftp=theSftp, thedayStr=thedayStr2, allFiles=None, fromRemoteDir=fromRemoteDir, toLocalDir=toLocalDir)

            except Exception as e:
                traceback.print_exc()
                logging.error(str(e))
                self.sftpLog('sftp 批量下载文件失败')
        else:
            self.sftpLog('sftp 连接失败')

        self.sftpLog('批量下载文件结束')

        if theSftp != 'default':
            try:
                theSftp.close()
                logging.info('sftp 关闭')
                print('sftp 关闭')
            except Exception as e2:
                print(e2)
            theSftp = 'default'

    def copyFiles(self, toDir, findStr='', fileNames=None, fromDir=''):
        # 设置默认值
        if toDir is None or len(toDir) == 0:
            return
        if fromDir is None or len(fromDir) == 0:
            fromDir = self.localDir

        # 设置默认值
        self.sftpLog('纯文件复制开始 from:' + fromDir + ' to: '+toDir)

        self.safeMakedir(toDir)  # '/home/thjk01/thzc/'
        #        self.safeMakedir(dataClean.aimPath)  # '/home/thjk01/thzc/cleanedData/'

        fileList = myLocalFile.getchild(fromDir)

        for fromFile in fileList:
            shortname = os.path.basename(fromFile)
            toFile = os.path.join(toDir, shortname)

            # 下面这句话，只能拷贝名字在列表中的（列表为空则不管），或者文件包含findStr的文件
            if (fileNames is None or len(fileNames) == 0 or shortname in fileNames) and \
                    (findStr is None or len(findStr) == 0 or shortname.find(findStr) >= 0):
                if os.path.isfile(fromFile):
                    isdownloaded = False
                    stinfo1 = os.stat(fromFile)

                    if os.path.isfile(toFile):
                        stinfo2 = os.stat(toFile)

                        if (stinfo1.st_size == stinfo2.st_size and abs(
                                stinfo2.st_mtime - stinfo1.st_mtime) < 10):  # 本地文件时间是float
                            isdownloaded = True

                    if not isdownloaded:
                        shutil.copyfile(fromFile, toFile)

                        # 修改访问和修改时间
                        os.chmod(toFile, stat.S_IWOTH + stat.S_IROTH + stat.S_IWGRP + stat.S_IRGRP + stat.S_IWUSR + stat.S_IRUSR)
                        os.utime(toFile, (stinfo1.st_atime, stinfo1.st_mtime))

                        self.sftpLog('成功Copy文件 ' + toFile)
                    else:
                        self.sftpLog('已经存在 ' + toFile)
                else:
                    self.sftpLog('文件不存在 ' + fromFile)

        self.sftpLog('纯文件复制结束 from:' + fromDir + ' to: '+toDir)

    def copyFilesByDay(self, toDir, thedayStr='', fileNames=None, fromDir=''):
        # 检查默认值
        if toDir is None or len(toDir) == 0:
            return
        if fromDir is None or len(fromDir) == 0:
            fromDir = self.localDir
        thedayStr = self.getTheDateStr(thedayStr)
        # 检查默认值

        self.sftpLog('单日文件复制开始 ' + thedayStr)

        self.safeMakedir(toDir)  # '/home/thjk01/thzc/'
#        self.safeMakedir(dataClean.aimPath)  # '/home/thjk01/thzc/cleanedData/'

        realFileNames = []
        if isinstance(fileNames, list):
            for fileName in fileNames:
                realFileNames.append(fileName + thedayStr + '.csv')

        # self.copyFiles(toDir=os.path.join(toDir, thedayStr), findStr=thedayStr, fileNames=realFileNames, fromDir=os.path.join(fromDir, thedayStr))
        # 上面的是要文件名包含 20181115，下面的不用

        self.copyFiles(toDir=os.path.join(toDir, thedayStr), findStr='', fileNames=realFileNames, fromDir=os.path.join(fromDir, thedayStr))

    def copyFilesByRange(self, toDir, thedayStr='', days=1, fileNames=None, fromDir=''):
        # 检查默认值
        if toDir is None or len(toDir) == 0:
            return
        if fromDir is None or len(fromDir) == 0:
            fromDir = self.localDir
        thedayStr = self.getTheDateStr(thedayStr)
        date1 = self.getTheDate(thedayStr)
        # 检查默认值

        self.sftpLog('批量复制文件开始')

        for i in range(0, days):
            date2 = date1 - datetime.timedelta(days=(days - i - 1))
            thedayStr2 = date2.strftime("%Y%m%d")
            self.copyFilesByDay(toDir=toDir, thedayStr=thedayStr2, fileNames=fileNames, fromDir=fromDir)

        self.sftpLog('批量复制文件结束')

    def cleanFilesByDay(self, thedayStr=''):
        thedayStr = self.getTheDateStr(thedayStr)
        thedate = self.getTheDate(thedayStr)
        self.sftpLog(time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time())) + ' 文件清洗开始:clean、rename、append ' + thedayStr)

        dataClean.dataCleanTrustApply(thedayStr)
        dataClean.renameFiles(thedayStr)
        dataClean.appendData(thedate, thedayStr)

        self.sftpLog(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())) + ' 文件清洗结束 ' + thedayStr)

    def cleanFilesByRange(self, thedayStr='', days=1):
        thedayStr = self.getTheDateStr(thedayStr)
        date1 = self.getTheDate(thedayStr)

        self.sftpLog('批量文件清洗开始')

        for i in range(0, days):
            date2 = date1 - datetime.timedelta(days=(days - i - 1))
            thedayStr2 = date2.strftime("%Y%m%d")
            self.cleanFilesByDay(thedayStr=thedayStr2)

        self.sftpLog('批量文件清洗结束')
