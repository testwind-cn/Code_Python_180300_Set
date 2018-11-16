#!/usr/bin/python3
# -*- coding: UTF-8 -*-

import sys
import logging
import traceback
import os,stat
import shutil
import time
from apscheduler.schedulers.blocking import BlockingScheduler

import sftpUtil
import sftp_config
import paramiko
import datetime
import dataClean


def getTheDateStr(thedayStr=''):
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


def getTheDate(thedayStr=''):
    #    thedayStr = getTheDateStr(thedayStr)
    try:
        date1 = datetime.datetime.strptime(thedayStr, "%Y%m%d").date()
    except Exception as e:
        date1 = datetime.date.today()
    return date1


def getTheDateTick(thedayStr=''):
    #    thedayStr = getTheDateStr(thedayStr)
    try:
        stime = time.strptime(thedayStr, "%Y%m%d")
        thedatetick = time.mktime(stime)
    except Exception as e:
        date1 = datetime.date.today()
        thedatetick = time.mktime(date1.timetuple())
    return thedatetick


def safeMakedir(dirStr):
    try:
        if not os.path.exists(dirStr):
            os.makedirs(dirStr)
            os.chmod(dirStr, stat.S_IRWXO + stat.S_IRWXG + stat.S_IRWXU)
    except Exception as e:
        print(e)
    return


def downloadFilesByDay(theSftp='default', thedayStr='', allFiles=None):
    if (theSftp == 'default' or allFiles is None):
        return

    thedayStr = getTheDateStr(thedayStr)
    thedatetick1 = getTheDateTick(thedayStr)

    date2 = getTheDate(thedayStr) + datetime.timedelta(days=1)
    thedatetick2 = time.mktime(date2.timetuple())

    logging.info('单日文件下载开始' + thedayStr)
    print('单日文件下载开始' + thedayStr)

    safeMakedir(sftp_config.localDir)

    for aFile in allFiles:
        # aFile.filenmae        # aFile.st_atime        # aFile.st_mtime
        realFileName = aFile.filename
        filedate = aFile.filename[len(aFile.filename) - 12: len(aFile.filename) - 4]

        if (thedatetick1 <= aFile.st_mtime and aFile.st_mtime < thedatetick2 and filedate != thedayStr) or (
                filedate == thedayStr):
            safeMakedir(sftp_config.localDir + thedayStr)

            try:
                isdownloaded = False
                stinfo1 = theSftp.stat(sftp_config.homeDir + realFileName)
                # 可以用上面的 aFile 里的信息代替

                if os.path.exists(sftp_config.localDir + thedayStr + '/' + realFileName):
                    stinfo2 = os.stat(sftp_config.localDir + thedayStr + '/' + realFileName)
                    if (stinfo1.st_size == stinfo2.st_size and abs(
                            int(stinfo2.st_mtime) - stinfo1.st_mtime) < 10):  # 本地文件时间是float
                        isdownloaded = True

                if (isdownloaded == False):
                    theSftp.get(sftp_config.homeDir + realFileName,
                                sftp_config.localDir + thedayStr + '/' + realFileName)
                    # 修改访问和修改时间
                    os.chmod(sftp_config.localDir + thedayStr + '/' + realFileName,
                             stat.S_IWOTH + stat.S_IROTH + stat.S_IWGRP + stat.S_IRGRP + stat.S_IWUSR + stat.S_IRUSR)
                    os.utime(sftp_config.localDir + thedayStr + '/' + realFileName,
                             (stinfo1.st_atime, stinfo1.st_mtime))
                    logging.info('成功下载 ' + realFileName)
                    print('成功下载 ' + realFileName)
                else:
                    logging.info('已经存在 ' + realFileName)
                    print('已经存在 ' + realFileName)

            except Exception as e:
                traceback.print_exc()
                logging.error(str(e))
                logging.info('文件下载失败：' + realFileName)
                print('文件下载失败：' + realFileName)

    logging.info('单日文件下载结束 ' + thedayStr)
    print('单日文件下载结束 ' + thedayStr)


def downloadFilesByRange(thedayStr='', days=1):
    theSftp = 'default'

    thedayStr1 = getTheDateStr(thedayStr)
    date1 = getTheDate(thedayStr1)

    logging.info('批量下载文件开始')
    print('批量下载文件开始')

    result = sftpUtil.getConnect(sftp_config.host, sftp_config.port, sftp_config.username, sftp_config.password)

    if (result[0] == 1):
        try:
            theSftp = paramiko.SFTPClient.from_transport(result[2])
            allFiles = theSftp.listdir_attr(sftp_config.homeDir)

            if len(allFiles) > 0:
                for i in range(0, days):
                    date2 = date1 - datetime.timedelta(days=(days - i - 1))
                    thedayStr2 = date2.strftime("%Y%m%d")
                    downloadFilesByDay(theSftp=theSftp, thedayStr=thedayStr2, allFiles=allFiles)

        except Exception as e:
            traceback.print_exc()
            logging.error(str(e))
            logging.info('sftp 批量下载文件失败')
            print('sftp 批量下载文件失败')
    else:
        logging.info('sftp 连接失败')

    logging.info('批量下载文件结束')
    print('批量下载文件结束')

    if (theSftp != 'default'):
        try:
            theSftp.close()
            logging.info('sftp 关闭')
            print('sftp 关闭')
        except Exception as e2:
            print(e2)
        theSftp = 'default'


def copyFilesByDay(thedayStr=''):
    thedayStr = getTheDateStr(thedayStr)
    thedate = getTheDate(thedayStr)

    logging.info('单日文件复制开始 ' + thedayStr)
    print('单日文件复制开始 ' + thedayStr)

    safeMakedir(dataClean.srcPath)  # '/home/thjk01/thzc/'
    safeMakedir(dataClean.aimPath)  # '/home/thjk01/thzc/cleanedData/'

    for fileName in dataClean.needFiles:
        realFileName = fileName + thedayStr + '.csv'

        if os.path.isfile(sftp_config.localDir + thedayStr + '/' + realFileName):
            isdownloaded = False
            stinfo1 = os.stat(sftp_config.localDir + thedayStr + '/' + realFileName)

            if os.path.exists(dataClean.srcPath + realFileName):

                stinfo2 = os.stat(dataClean.srcPath + realFileName)
                if (stinfo1.st_size == stinfo2.st_size and abs(
                        stinfo2.st_mtime - stinfo1.st_mtime) < 10):  # 本地文件时间是float
                    isdownloaded = True

            if (isdownloaded == False):
                shutil.copyfile(sftp_config.localDir + thedayStr + '/' + realFileName, dataClean.srcPath + realFileName)

                # 修改访问和修改时间
                os.chmod(dataClean.srcPath + realFileName,
                         stat.S_IWOTH + stat.S_IROTH + stat.S_IWGRP + stat.S_IRGRP + stat.S_IWUSR + stat.S_IRUSR)
                os.utime(dataClean.srcPath + realFileName, (stinfo1.st_atime, stinfo1.st_mtime))

                logging.info('成功Copy文件 ' + realFileName)
                print('成功Copy文件 ' + realFileName)
            else:
                logging.info('已经存在 ' + realFileName)
                print('已经存在 ' + realFileName)
        else:
            logging.info('文件不存在 ' + realFileName)
            print('文件不存在 ' + realFileName)

    logging.info('文件清洗开始 ' + thedayStr + ' :clean、rename、append')
    print(time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()) ) + ' 文件清洗开始:clean、rename、append ' + thedayStr )
    dataClean.dataCleanTrustApply(thedayStr)
    dataClean.renameFiles(thedayStr)
    dataClean.appendData(thedate, thedayStr)
    logging.info('文件清洗结束 ' + thedayStr)
    print(time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()) ) + ' 文件清洗结束 ' + thedayStr )


def copyFilesByRange(thedayStr='', days=1):
    thedayStr = getTheDateStr(thedayStr)
    date1 = getTheDate(thedayStr)

    logging.info('批量复制文件开始')
    print('批量复制文件开始')

    for i in range(0, days):
        date2 = date1 - datetime.timedelta(days=(days - i - 1))
        thedayStr2 = date2.strftime("%Y%m%d")
        copyFilesByDay(thedayStr=thedayStr2)

    logging.info('批量复制文件结束')
    print('批量复制文件结束')


def scanTask(todayStr=''):
    sftp = 'default'

    try:
        logging.info('数据下载开始')

        if len(todayStr) <= 0:
            curYear = 2018
            today = datetime.date.today()
            today = today.replace(curYear, today.month, today.day)
            todayStr = str(today).replace('-', '')

        result = sftpUtil.getConnect(sftp_config.host, sftp_config.port, sftp_config.username, sftp_config.password)

        if (result[0] == 1):
            sftp = paramiko.SFTPClient.from_transport(result[2])
            allFiles = sftp.listdir(sftp_config.homeDir)
            allFiles2 = sftp.listdir_attr(sftp_config.homeDir)

            if not os.path.exists(sftp_config.localDir):
                os.makedirs(sftp_config.localDir)

            if not os.path.exists(sftp_config.localDir + todayStr):
                os.makedirs(sftp_config.localDir + todayStr)

            if not os.path.exists(dataClean.srcPath):  # '/home/thjk01/thzc/'
                os.makedirs(dataClean.srcPath)

            if not os.path.exists(dataClean.aimPath):  # '/home/thjk01/thzc/cleanedData/'
                os.makedirs(dataClean.aimPath)

            for fileName in dataClean.needFiles:
                realFileName = fileName + todayStr + '.csv'
                if (realFileName in allFiles):
                    #                    sftp.get(sftp_config.homeDir+realFileName, sftp_config.localDir+realFileName)
                    sftp.get(sftp_config.homeDir + realFileName, sftp_config.localDir + todayStr + '/' + realFileName)

                    shutil.copyfile(sftp_config.localDir + todayStr + '/' + realFileName,
                                    dataClean.srcPath + realFileName)

                    stinfo = sftp.stat(sftp_config.homeDir + realFileName)
                    # 修改访问和修改时间
                    os.utime(dataClean.srcPath + realFileName, (stinfo.st_atime, stinfo.st_mtime))
                    os.utime(sftp_config.localDir + todayStr + '/' + realFileName, (stinfo.st_atime, stinfo.st_mtime))
                    #                    os.utime(sftp_config.localDir + realFileName, (stinfo.st_atime, stinfo.st_mtime))

                    logging.info('成功下载 ' + realFileName)
                    print('成功下载 ' + realFileName)
                else:
                    logging.info('文件不存在 ' + realFileName)
                    print('文件不存在 ' + realFileName)

            dataClean.dataCleanTrustApply(todayStr)
            dataClean.renameFiles(todayStr)
            dataClean.appendData(today, todayStr)

        else:
            logging.info('sftp 连接失败')

    except Exception as e:
        traceback.print_exc()
        logging.error(str(e))
    finally:
        if (sftp != 'default'):
            sftp.close()


if __name__ == "__main__":

    thedayStr = ''
    days = 1

    np = len(sys.argv)
    print(np)
    print(str(sys.argv))
    if np >= 2:
        thedayStr = sys.argv[1]
    if np >= 3:
        if sys.argv[2].isdecimal():
            days = int(sys.argv[2])
        else:
            days = 1

    logging.basicConfig(filename='sftpLog.log',
                        format='%(asctime)s -%(name)s-%(levelname)s-%(module)s:%(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S',
                        level=logging.INFO)

    #    scheduler = BlockingScheduler()
    #    scheduler.add_job(scanTask, 'cron', day='*/1', hour='9', minute='5', second='0')

    try:
        print('start')
        # scheduler.start()

        downloadFilesByRange(thedayStr=thedayStr, days=days)
        copyFilesByRange(thedayStr=thedayStr, days=days)

        #        scanTask()

        print('end')
    except Exception as e:
        #        scheduler.shutdown()
        #        traceback.print_exc()
        logging.error(str(e))
