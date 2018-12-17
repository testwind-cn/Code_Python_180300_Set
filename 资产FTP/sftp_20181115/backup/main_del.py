import os
import shutil

import sftp_config
import paramiko
import datetime
import dataClean
from wj_tools import sftpUtil
from wj_tools.mylog import myLog

def scanTask(todayStr=''):
    sftp = 'default'

    try:
        myLog.info('数据下载开始')

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
                    #                    sftp.get(sftp_config.homeDir+realFileName, sftp_config.__m_localDir+realFileName)
                    sftp.get(sftp_config.remoteDir + realFileName, sftp_config.localDir + todayStr + '/' + realFileName)

                    shutil.copyfile(sftp_config.localDir + todayStr + '/' + realFileName,
                                    dataClean.srcPath + realFileName)

                    stinfo = sftp.stat(sftp_config.remoteDir + realFileName)
                    # 修改访问和修改时间
                    os.utime(dataClean.srcPath + realFileName, (stinfo.st_atime, stinfo.st_mtime))
                    os.utime(sftp_config.localDir + todayStr + '/' + realFileName, (stinfo.st_atime, stinfo.st_mtime))
                    #                    os.utime(sftp_config.__m_localDir + realFileName, (stinfo.st_atime, stinfo.st_mtime))

                    myLog.info('成功下载 ' + realFileName)
                    print('成功下载 ' + realFileName)
                else:
                    myLog.info('文件不存在 ' + realFileName)
                    print('文件不存在 ' + realFileName)

            dataClean.dataCleanTrustApply(todayStr)
            dataClean.renameFiles(todayStr)
            dataClean.appendData(today, todayStr)

        else:
            myLog.info('sftp 连接失败')

    except Exception as e:
        myLog.error(str(e))
    finally:
        if (sftp != 'default'):
            sftp.close()
