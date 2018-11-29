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
            allFiles = sftp.listdir(sftp_config.remoteDir)
            allFiles2 = sftp.listdir_attr(sftp_config.remoteDir)

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
                    sftp.get(sftp_config.remoteDir + realFileName, sftp_config.localDir + todayStr + '/' + realFileName)

                    shutil.copyfile(sftp_config.localDir + todayStr + '/' + realFileName,
                                    dataClean.srcPath + realFileName)

                    stinfo = sftp.stat(sftp_config.remoteDir + realFileName)
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
        aSftp = sftp_config.Sftp_Tool()
        aSftp.downloadFilesByRange(thedayStr=thedayStr, days=days, fromRemoteDir=aSftp.remoteDir, toLocalDir=aSftp.localDir)
        dataClean.srcPath = "D:/stp_clean"
        aSftp.copyFilesByRange(toDir=dataClean.srcPath, thedayStr=thedayStr, days=days, fileNames=None, fromDir=aSftp.localDir)
        # copyFilesByRange(fileNames=dataClean.needFiles,
        aSftp.cleanFilesByRange(thedayStr=thedayStr, days=days)
        #        scanTask()

        print('end')
    except Exception as e:
        #        scheduler.shutdown()
        #        traceback.print_exc()
        logging.error(str(e))
