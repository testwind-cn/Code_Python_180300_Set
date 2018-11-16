# -*- coding: utf-8 -*-
"""
Created on Mon Aug  8 15:58:20 2016

@author: admin
"""
import datetime
import pandas as pd
import os
import shutil
import calendar

srcPath='E:\\sftp\\'
aimPath='E:\\sftp\\cleanedData\\'

#nowYear=2018
#today=datetime.date.today()
#today=today.replace(nowYear,today.month,today.day)
#todayStr=str(today).replace('-','')

def renameFile(srcName,aimName,todayStr):
    if os.path.exists(srcPath+srcName+todayStr+'.csv'):
        shutil.copyfile(srcPath+srcName+todayStr+'.csv',aimPath+aimName+todayStr+'.csv')
        
def fileAppendData(baseFilePath,appendFilePath):
    if os.path.exists(baseFilePath) and os.path.exists(appendFilePath):
        appendFile=pd.read_csv(appendFilePath,low_memory=False,encoding="gb18030")
        appendFile.to_csv(baseFilePath,index=False,header=False, mode='a+')
        appendFile=None

def dataCleanTrustApply(todayStr):
    if os.path.exists(srcPath+'DB2_POSL_POSL_TRUST_APPLY_'+todayStr+'.csv'):
        file=pd.read_csv(srcPath+'DB2_POSL_POSL_TRUST_APPLY_'+todayStr+'.csv',low_memory=False,encoding="gb18030")
        var=file['LIVE_ADDR'][file['ENTITY_OID']==82286].values[0]
        var=var.replace('\x1a','')
        file.loc[file['ENTITY_OID']==82286,'LIVE_ADDR']=var
        file.to_csv(aimPath+'POSL_TRUST_APPLY_'+todayStr+'.csv',index=False)
        file=None

def renameFiles(todayStr):
    srcNameArr=['DB2_POSL_BATCH_BOS_DEDUCT_',
        'DB2_POSL_BATCH_BOS_DEDUCT_BALANCE_',
        'DB2_POSL_BATCH_BOS_EARLY_BALANCE_',
        'DB2_POSL_BATCH_BOS_GRANT_BALANCE_',
        'DB2_POSL_BATCH_SPD_DEDUCT_',
        'DB2_POSL_BATCH_SPD_DEDUCT_BALANCE_',
        'DB2_POSL_BATCH_SPD_EARLY_BALANCE_',
        'DB2_POSL_BATCH_SPD_GRANT_BALANCE_',
        'DB2_POSL_POSL_LOAN_APPLY_',
        'MYSQL_LOAN_T_LOAN_CREDIT_REQ_',
        'MYSQL_LOAN_T_LOAN_EARLYREPAY_REQ_',
        'MYSQL_LOAN_T_LOAN_OUTSIDE_MCHT_RIG_',
        'MYSQL_LOAN_T_LOAN_USE_REQ_',
        'MYSQL_LOAN_T_REOCN_CREDIT_',
        'MYSQL_LOAN_T_REOCN_DEDUCT_',
        'MYSQL_LOAN_T_REOCN_DEDUCT_BALANCE_',
        'MYSQL_LOAN_T_REOCN_EARLYREPAY_',
        'MYSQL_LOAN_T_REOCN_LOANGRANT_']
    
    aimNameArr=['BATCH_BOS_DEDUCT_',
        'BATCH_BOS_DEDUCT_BALANCE_',
        'BATCH_BOS_EARLY_BALANCE_',
        'BATCH_BOS_GRANT_BALANCE_',
        'BATCH_SPD_DEDUCT_',
        'BATCH_SPD_DEDUCT_BALANCE_',
        'BATCH_SPD_EARLY_BALANCE_',
        'BATCH_SPD_GRANT_BALANCE_',
        'POSL_LOAN_APPLY_',
        'T_LOAN_CREDIT_REQ_',
        'T_LOAN_EARLYREPAY_REQ_',
        'T_LOAN_OUTSIDE_MCHT_RIG_',
        'T_LOAN_USE_REQ_',
        'T_REOCN_CREDIT_',
        'T_REOCN_DEDUCT_',
        'T_REOCN_DEDUCT_BALANCE_',
        'T_REOCN_EARLYREPAY_',
        'T_REOCN_LOANGRANT_']
    
    for src,aim in zip(srcNameArr,aimNameArr):
        renameFile(src,aim,todayStr)

def appendData(today,todayStr): 
    if today.day<21:
        endDayStr=str(today.replace(today.year,today.month,20)).replace('-','')
        if today.day==1:
            if os.path.exists(srcPath+'DB2_POSL_APMS_LOGINFO_'+todayStr+'.csv'):
                shutil.copyfile(srcPath+'DB2_POSL_APMS_LOGINFO_'+todayStr+'.csv',aimPath+'APMS_LOGINFO'+endDayStr+'.csv')
        else:
            if os.path.exists(aimPath+'APMS_LOGINFO'+endDayStr+'.csv'):    
                fileAppendData(aimPath+'APMS_LOGINFO'+endDayStr+'.csv',srcPath+'DB2_POSL_APMS_LOGINFO_'+todayStr+'.csv')
            else:
                shutil.copyfile(srcPath+'DB2_POSL_APMS_LOGINFO_'+todayStr+'.csv',aimPath+'APMS_LOGINFO'+endDayStr+'.csv')
            
    else:
        endDayStr=str(today.replace(today.year,today.month,calendar.monthrange(today.year,today.month)[1])).replace('-','')
        if today.day==20:
            if os.path.exists(srcPath+'DB2_POSL_APMS_LOGINFO_'+todayStr+'.csv'):
                shutil.copyfile(srcPath+'DB2_POSL_APMS_LOGINFO_'+todayStr+'.csv',aimPath+'APMS_LOGINFO'+endDayStr+'.csv')
        else:
            if os.path.exists(aimPath+'APMS_LOGINFO'+endDayStr+'.csv'):    
                fileAppendData(aimPath+'APMS_LOGINFO'+endDayStr+'.csv',srcPath+'DB2_POSL_APMS_LOGINFO_'+todayStr+'.csv')
            else:
                shutil.copyfile(srcPath+'DB2_POSL_APMS_LOGINFO_'+todayStr+'.csv',aimPath+'APMS_LOGINFO'+endDayStr+'.csv')

    weekDay=today.weekday()
    if weekDay<=4:
        addDay=4-weekDay
    else:
        addDay=11-weekDay
    loanEndDayStr=str(today.replace(today.year,today.month,today.day+addDay)).replace('-','')
    if os.path.exists(srcPath+'MYSQL_LOAN_T_REOCN_LOANINFO_'+todayStr+'.csv'):
        if os.path.exists(aimPath+'T_REOCN_LOANINFO_'+loanEndDayStr+'.csv'):
            fileAppendData(aimPath+'T_REOCN_LOANINFO_'+loanEndDayStr+'.csv',srcPath+'MYSQL_LOAN_T_REOCN_LOANINFO_'+todayStr+'.csv')
        else:
            shutil.copyfile(srcPath+'MYSQL_LOAN_T_REOCN_LOANINFO_'+todayStr+'.csv',aimPath+'T_REOCN_LOANINFO_'+loanEndDayStr+'.csv')

#单独修正数据使用
def singleAppendData(year,month,day):
    today=datetime.date.today()
    today=today.replace(year,month,day)
    todayStr=str(today).replace('-','')
    appendData(today,todayStr)