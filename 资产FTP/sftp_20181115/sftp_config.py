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

# SFTP服务器的IP、端口、账户、密码
host = "172.31.71.71"
port = 12306  # not str
username = "yuanxj"
password = "Uwj1qsFnV8"

# remote和local是相对客户端的
remoteDir = "/tmp/"
# ed
# localDir = "E:/sftp_20181115/"
localDir = "/home/data/thzc/"


class Sftp_Tool:
    # SFTP服务器的IP、端口、账户、密码
    host = "172.31.71.71"
    port = 12306  # not str
    username = "yuanxj"
    password = "Uwj1qsFnV8"

    # remote和local是相对客户端的
    remoteDir = "/tmp/"
    # ed
    # localDir = "E:/sftp_20181115/"
    localDir = "/home/data/thzc/"

    def sftpLog(self,info=''):
        logging.info(info)
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
            print(e)
        return

#    sftp_config.remoteDir
#    sftp_config.localDir

    def downloadFilesByDay(self,theSftp='default', thedayStr='', allFiles=None, fromRemoteDir='', toLocalDir=''):
        if theSftp == 'default' or allFiles is None:
            return

        if fromRemoteDir is None or len(fromRemoteDir) == 0:
            fromRemoteDir = self.remoteDir

        if toLocalDir is None or len(toLocalDir) == 0:
            toLocalDir = self.localDir

        thedayStr = self.getTheDateStr(thedayStr)
        thedatetick1 = self.getTheDateTick(thedayStr)

        date2 = self.getTheDate(thedayStr) + datetime.timedelta(days=1)
        thedatetick2 = time.mktime(date2.timetuple())

        self.sftpLog('单日文件下载开始' + thedayStr)

        self.safeMakedir(toLocalDir)

        for aFile in allFiles:
            # aFile.filenmae        # aFile.st_atime        # aFile.st_mtime
            realFileName = aFile.filename
            filedate = aFile.filename[len(aFile.filename) - 12: len(aFile.filename) - 4]

            if ( thedatetick1 <= aFile.st_mtime and aFile.st_mtime < thedatetick2 and filedate != thedayStr) or (
                    filedate == thedayStr):
                self.safeMakedir(toLocalDir + thedayStr)

                try:
                    isdownloaded = False
                    stinfo1 = theSftp.stat(fromRemoteDir + realFileName)
                    # 可以用上面的 aFile 里的信息代替

                    if os.path.exists(toLocalDir + thedayStr + '/' + realFileName):
                        stinfo2 = os.stat(toLocalDir + thedayStr + '/' + realFileName)
                        if (stinfo1.st_size == stinfo2.st_size and abs(
                                int(stinfo2.st_mtime) - stinfo1.st_mtime) < 10):  # 本地文件时间是float
                            isdownloaded = True

                    if isdownloaded == False:
                        theSftp.get(fromRemoteDir + realFileName,
                                    toLocalDir + thedayStr + '/' + realFileName)
                        # 修改访问和修改时间
                        os.chmod(toLocalDir + thedayStr + '/' + realFileName,
                                 stat.S_IWOTH + stat.S_IROTH + stat.S_IWGRP + stat.S_IRGRP + stat.S_IWUSR + stat.S_IRUSR)
                        os.utime(toLocalDir + thedayStr + '/' + realFileName,
                                 (stinfo1.st_atime, stinfo1.st_mtime))
                        self.sftpLog('成功下载 ' + realFileName)
                    else:
                        self.sftpLog('已经存在 ' + realFileName)

                except Exception as e:
                    traceback.print_exc()
                    logging.error(str(e))
                    self.sftpLog('文件下载失败：' + realFileName)

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
                allFiles = theSftp.listdir_attr( fromRemoteDir )

                if len(allFiles) > 0:
                    for i in range(0, days):
                        date2 = date1 - datetime.timedelta(days=(days - i - 1))
                        thedayStr2 = date2.strftime("%Y%m%d")
                        self.downloadFilesByDay(theSftp=theSftp, thedayStr=thedayStr2, allFiles=allFiles, fromRemoteDir=fromRemoteDir, toLocalDir=toLocalDir)

            except Exception as e:
                traceback.print_exc()
                logging.error(str(e))
                self.sftpLog('sftp_20181115 批量下载文件失败')
        else:
            self.sftpLog('sftp_20181115 连接失败')

        self.sftpLog('批量下载文件结束')

        if theSftp != 'default':
            try:
                theSftp.close()
                logging.info('sftp_20181115 关闭')
                print('sftp_20181115 关闭')
            except Exception as e2:
                print(e2)
            theSftp = 'default'

    def copyFiles(self, fileNames, toDir, fromDir=''):
        # 设置默认值
        if toDir is None or len(toDir) == 0 or fileNames is None or len(fileNames) == 0:
            return
        if fromDir is None or len(fromDir) == 0:
            fromDir = self.localDir

        # 设置默认值

        self.sftpLog('纯文件复制开始 from:' + fromDir + ' to: '+toDir)

        self.safeMakedir(toDir)  # '/home/thjk01/thzc/'
        #        self.safeMakedir(dataClean.aimPath)  # '/home/thjk01/thzc/cleanedData/'

        for fileName in fileNames:
            realFileName = fileName
            if os.path.isfile(fromDir + realFileName):
                isdownloaded = False
                stinfo1 = os.stat(fromDir + realFileName)

                if os.path.exists(toDir + realFileName):
                    stinfo2 = os.stat(toDir + realFileName)
                    if (stinfo1.st_size == stinfo2.st_size and abs(
                            stinfo2.st_mtime - stinfo1.st_mtime) < 10):  # 本地文件时间是float
                        isdownloaded = True

                if (isdownloaded == False):
                    shutil.copyfile(fromDir + realFileName, toDir + realFileName)

                    # 修改访问和修改时间
                    os.chmod(toDir + realFileName, stat.S_IWOTH + stat.S_IROTH + stat.S_IWGRP + stat.S_IRGRP + stat.S_IWUSR + stat.S_IRUSR)
                    os.utime(toDir + realFileName, (stinfo1.st_atime, stinfo1.st_mtime))

                    self.sftpLog('成功Copy文件 ' + realFileName)
                else:
                    self.sftpLog('已经存在 ' + realFileName)
            else:
                self.sftpLog('文件不存在 ' + realFileName)

        self.sftpLog('纯文件复制结束 from:' + fromDir + ' to: ' + toDir)

    def copyFilesByDay(self, fileNames, toDir, thedayStr='',fromDir=''):
        # 设置默认值
        if toDir is None or len(toDir) == 0 or fileNames is None or len(fileNames) == 0:
            return
        if fromDir is None or len(fromDir) == 0:
            fromDir = self.localDir
        thedayStr = self.getTheDateStr(thedayStr)
        # 设置默认值

        self.sftpLog('单日文件复制开始 ' + thedayStr)

        self.safeMakedir(toDir)  # '/home/thjk01/thzc/'
#        self.safeMakedir(dataClean.aimPath)  # '/home/thjk01/thzc/cleanedData/'

        realFileNames = []
        for fileName in fileNames:
            realFileNames.append(fileName + thedayStr + '.csv')

        self.copyFiles(realFileNames,toDir=toDir,fromDir=fromDir+thedayStr+'/')

    def copyFilesByRange(self, fileNames, toDir, thedayStr='', days=1, fromDir=''):
        if toDir is None or len(toDir) == 0 or fileNames is None or len(fileNames) == 0:
            return
        if fromDir is None or len(fromDir) == 0:
            fromDir = self.localDir

        thedayStr = self.getTheDateStr(thedayStr)
        date1 = self.getTheDate(thedayStr)

        self.sftpLog('批量复制文件开始')

        for i in range(0, days):
            date2 = date1 - datetime.timedelta(days=(days - i - 1))
            thedayStr2 = date2.strftime("%Y%m%d")
            self.copyFilesByDay(fileNames=fileNames, toDir=toDir,thedayStr=thedayStr2, fromDir=fromDir)

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
