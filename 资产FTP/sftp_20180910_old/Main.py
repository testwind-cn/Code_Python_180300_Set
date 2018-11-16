import logging
import traceback

from apscheduler.schedulers.blocking import BlockingScheduler

import sftpUtil
import sftp_config
import paramiko
import datetime
import dataClean

logging.basicConfig(filename='sftpLog.log',
                    format='%(asctime)s -%(name)s-%(levelname)s-%(module)s:%(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    level=logging.INFO)

def scanTask():

    sftp='default'
    try:
        
        logging.info('数据下载开始')
        
        curYear=2018
        today=datetime.date.today()
        today=today.replace(curYear,today.month,today.day)
        todayStr=str(today).replace('-','')
        
        result=sftpUtil.getConnect(sftp_config.host,sftp_config.port,sftp_config.username,sftp_config.password)
        if(result[0]==1):
            sftp = paramiko.SFTPClient.from_transport(result[2])
            allFiles=sftp.listdir(sftp_config.homeDir)            
            needFiles=[
            'DB2_POSL_BATCH_BOS_DEDUCT_',
            'DB2_POSL_BATCH_BOS_DEDUCT_BALANCE_',
            'DB2_POSL_BATCH_BOS_EARLY_BALANCE_',
            'DB2_POSL_BATCH_BOS_GRANT_BALANCE_',
            'DB2_POSL_BATCH_SPD_DEDUCT_',
            'DB2_POSL_BATCH_SPD_DEDUCT_BALANCE_',
            'DB2_POSL_BATCH_SPD_EARLY_BALANCE_',
            'DB2_POSL_BATCH_SPD_GRANT_BALANCE_',
            'DB2_POSL_POSL_LOAN_APPLY_',
            'DB2_POSL_POSL_TRUST_APPLY_',
            'DB2_POSL_COMPENSATORY_',
            'DB2_POSL_APMS_LOGINFO_',
            'MYSQL_LOAN_T_LOAN_COMPERNSATORY_',
            'MYSQL_LOAN_T_LOAN_CREDIT_REQ_',
            'MYSQL_LOAN_T_LOAN_EARLYREPAY_REQ_',
            'MYSQL_LOAN_T_LOAN_OUTSIDE_MCHT_RIG_',
            'MYSQL_LOAN_T_LOAN_USE_REQ_',
            'MYSQL_LOAN_T_REOCN_CREDIT_',
            'MYSQL_LOAN_T_REOCN_DEDUCT_',
            'MYSQL_LOAN_T_REOCN_DEDUCT_BALANCE_',
            'MYSQL_LOAN_T_REOCN_EARLYREPAY_',
            'MYSQL_LOAN_T_REOCN_LOANGRANT_',
            'MYSQL_LOAN_T_REOCN_LOANINFO_'
            ]
            
            for fileName in needFiles:
                realFileName=fileName+todayStr+'.csv'
                if(realFileName in allFiles):
                    sftp.get(sftp_config.homeDir+realFileName,sftp_config.localDir+realFileName)
                    logging.info('成功下载 '+realFileName)
                else:
                    logging.info('文件不存在 '+realFileName)
            
            dataClean.dataCleanTrustApply(todayStr)
            dataClean.renameFiles(todayStr)
            dataClean.appendData(today,todayStr)
            
        else:
            logging.info('sftp 连接失败')
            
    except Exception as e:
        traceback.print_exc()
        logging.error(str(e))
    finally:
        if(sftp!='default'):
            sftp.close()

    
if __name__=="__main__":
    scheduler = BlockingScheduler()
    scheduler.add_job(scanTask, 'cron', day='*/1', hour='9', minute='5', second='0')
    
    try:
        print('start')
        scheduler.start()
        #scanTask()
        print('end')
    except Exception as e:
        scheduler.shutdown()
        traceback.print_exc()
        logging.error(str(e))