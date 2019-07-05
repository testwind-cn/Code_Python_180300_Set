#!coding:utf-8

import pandas as pd
from impala.dbapi import connect
from impala.util import as_pandas
from hdfs.client import Client
from impala.dbapi import connect
from wj_tools import sftp_tool
from wj_tools.file_check import MyLocalFile
from wj_tools.file_check import MyHdfsFile
from wj_tools.hdfsclient import MyClient  # hdfs
# hive
# data path config file
from conf import ConfigData
from wj_tools.str_tool import StrTool


def run_hive(conf: ConfigData, the_date: str):
    conn = connect(host="10.91.1.20", port=conf.hive_port(), auth_mechanism=conf.hive_auth(), user=conf.hive_user())
    cur = conn.cursor()

    sql = """
    --deduct union sql created by Zhaohu on 14-Nov-2018 am 10:49
use loginfo
"""
    print("OK" + "  " + sql + "\n")
    cur.execute(sql)  # , async=True)

    sql = "set mapreduce.job.queuename = deduct_union_gansu_mysql;"
    print("OK" + "  " + sql + "\n")
    cur.execute(sql)  # , async=True)
    sql = "set hive.cli.print.header=true;"
    print("OK" + "  " + sql + "\n")
    cur.execute(sql)  # , async=True)

    sql = """
select distinct 
case when creq.prod_code='8005000001' then '甘肃银行' else '' end as `产品类别`,
creq.LMT_SERNO as `授信编号`,
creq.CUST_NAME as `客户姓名`,
creq.LIVE_ADDR as `居住地`,
creq.ADDRESS as `客户联系地址`,
creq.TEL_NO as `家庭电话（贷款申请表）`,
creq.MOBILE as `手机号码（贷款申请表）`,
creq.MOBILE as `客户手机`,
creq.SPOUSE_NM as `配偶姓名`,
creq.SPOUSE_PHONE as `配偶电话`,
creq.EMER_NAME_1 as `第一联系人姓名`,
case when creq.EMER_REL_1='1' or creq.EMER_REL_1='01' then '配偶'
when creq.EMER_REL_1='2' or creq.EMER_REL_1='02' then '父母'
when creq.EMER_REL_1='3' or creq.EMER_REL_1='03' then '子女'
when creq.EMER_REL_1='4' or creq.EMER_REL_1='04' then '亲戚'
when creq.EMER_REL_1='5' or creq.EMER_REL_1='05' then '朋友'
when creq.EMER_REL_1='6' or creq.EMER_REL_1='06' then '其他'
when creq.EMER_REL_1='7' or creq.EMER_REL_1='07' then '兄弟姐妹'
when creq.EMER_REL_1='8' or creq.EMER_REL_1='08' then '同事'
else '' end as `第一联系人关系`,
creq.EMER_PHONE_1 as `第一联系人联系电话`,
creq.EMER_NAME_2 as `第二联系人姓名`,
case when creq.EMER_REL_2='1' or creq.EMER_REL_2='01' then '配偶'
when creq.EMER_REL_2='2' or creq.EMER_REL_2='02' then '父母'
when creq.EMER_REL_2='3' or creq.EMER_REL_2='03' then '子女'
when creq.EMER_REL_2='4' or creq.EMER_REL_2='04' then '亲戚'
when creq.EMER_REL_2='5' or creq.EMER_REL_2='05' then '朋友'
when creq.EMER_REL_2='6' or creq.EMER_REL_2='06' then '其他'
when creq.EMER_REL_2='7' or creq.EMER_REL_2='07' then '兄弟姐妹'
when creq.EMER_REL_2='8' or creq.EMER_REL_2='08' then '同事'
else '' end as `第二联系人关系`,
creq.EMER_PHONE_2 as `第二联系人联系电话`,
creq.EMER_NAME_3 as `第三联系人姓名`,
case when creq.EMER_REL_3='1' or creq.EMER_REL_3='01' then '配偶'
when creq.EMER_REL_3='2' or creq.EMER_REL_3='02' then '父母'
when creq.EMER_REL_3='3' or creq.EMER_REL_3='03' then '子女'
when creq.EMER_REL_3='4' or creq.EMER_REL_3='04' then '亲戚'
when creq.EMER_REL_3='5' or creq.EMER_REL_3='05' then '朋友'
when creq.EMER_REL_3='6' or creq.EMER_REL_3='06' then '其他'
when creq.EMER_REL_3='7' or creq.EMER_REL_3='07' then '兄弟姐妹'
when creq.EMER_REL_3='8' or creq.EMER_REL_3='08' then '同事'
else '' end as `第三联系人关系`,
creq.EMER_PHONE_3 as `第三联系人联系电话`,
creq.CREDIT_CONTRACT as `合同编号`,
trim(creq.CERT_NO) as `身份证号`,
case when creq.sex='2' then '男' when creq.sex='3' then '女' else '' end as `性别`,
loaninfo.limit_statr_date as `贷款开始日期`,
ureq.APP_START_DATE as `支用开始SAS日期`,
ureq.APP_END_DATE as `支用结束SAS日期`,
ureq.USE_DATE as `支用申请期限`,

merchant.contact_tel as `联系电话（APMS系统）`,
merchant.legal_name as `法定代表人姓名`,
merchant.finance_name as `财务联系人`,
merchant.finance_hp_no as `财务联系人电话（APMS系统）`,
merchant.stlm_acct as `还款银行卡号`,
merchant.name_busi as `商户名称`,
dict4.dict_name as `所在城市`,
merchant.busi_addr as `营业地址`,
dict1.dict_name as `分公司`,	
merchant.stlm_ins_city as `地市业务部`,
merchant.contact as `客户姓名`,
trim(creq.merchant_no) as `商户编号`,
dict2.dict_name as `inst_oid中文`,
dict3.dict_name as `mcc中文`,

trim(deductall.bill_no) as `借据编号`,
deductall.CURR_DATE as `截止日期`,

loaninfo.LOAN_AMT as `放款金额`,
loaninfo.LIMIT_END_DATE as `货款到期日`,
loaninfo.REMAIN_LOAN_AMT as `贷款剩余本金`,
loaninfo.REMAIN_LOAN_AMT as `应收未收本金`,
loaninfo.OVERDUE_INTE_AMT as `应收未收利息`,



case when compensate.BILL_NO is not null then '是'	else '否' end as `代偿状态`,
deductall.begin_late_date as `逾期开始日期`,
date_add(deductall.begin_late_date,1) as `新增日期`,
datediff(from_unixtime(unix_timestamp(deductall.CURR_DATE,'yyyymmdd'),'yyyy-mm-dd')
,deductall.begin_late_date) as `逾期天数`

from loginfo.union_flag1 deductall 
left join loginfo.t_loan_use_req  ureq 
on ureq.bill_no=trim(deductall.bill_no)
 left join loginfo.t_loan_credit_req creq on creq.LMT_SERNO=ureq.LMT_SERNO	
 left join loginfo.merchant_zc merchant on creq.merchant_no=merchant.mcht_cd
 left JOIN loginfo.T_REOCN_LOANINFO loaninfo on trim(deductall.bill_no)=loaninfo.bill_no and loaninfo.current_data=trim(deductall.curr_date)
left join loginfo.t_loan_compernsatory compensate on ureq.BILL_NO=compensate.BILL_NO
left join loginfo.t_allinpay_dict dict1 on merchant.aip_bran_id = dict1.dict_id and dict1.dict_type='aip_bran_id'
left join loginfo.t_allinpay_dict dict2 on merchant.inst_oid = dict2.dict_id and dict2.dict_type='inst_oid'
left join loginfo.t_allinpay_dict dict3 on merchant.up_mcc_cd = dict3.dict_id and dict3.dict_type='up_mcc_cd'
left join loginfo.t_allinpay_dict dict4 on merchant.city_cd = dict4.dict_id and dict4.dict_type='city_cd'
where creq.prod_code='8005000001'
    """

    sql2 = """
    select distinct 
case when creq.prod_code='8005000001' then '甘肃银行' else '' end as `产品类别`,
creq.LMT_SERNO as `授信编号`,
creq.CUST_NAME as `客户姓名`,
creq.LIVE_ADDR as `居住地`,
creq.ADDRESS as `客户联系地址`,
creq.TEL_NO as `家庭电话（贷款申请表）`,
creq.MOBILE as `手机号码（贷款申请表）`,
creq.MOBILE as `客户手机`,
creq.SPOUSE_NM as `配偶姓名`,
creq.SPOUSE_PHONE as `配偶电话`,
creq.EMER_NAME_1 as `第一联系人姓名`,
case when creq.EMER_REL_1='1' or creq.EMER_REL_1='01' then '配偶'
when creq.EMER_REL_1='2' or creq.EMER_REL_1='02' then '父母'
when creq.EMER_REL_1='3' or creq.EMER_REL_1='03' then '子女'
when creq.EMER_REL_1='4' or creq.EMER_REL_1='04' then '亲戚'
when creq.EMER_REL_1='5' or creq.EMER_REL_1='05' then '朋友'
when creq.EMER_REL_1='6' or creq.EMER_REL_1='06' then '其他'
when creq.EMER_REL_1='7' or creq.EMER_REL_1='07' then '兄弟姐妹'
when creq.EMER_REL_1='8' or creq.EMER_REL_1='08' then '同事'
else '' end as `第一联系人关系`,
creq.EMER_PHONE_1 as `第一联系人联系电话`,
creq.EMER_NAME_2 as `第二联系人姓名`,
case when creq.EMER_REL_2='1' or creq.EMER_REL_2='01' then '配偶'
when creq.EMER_REL_2='2' or creq.EMER_REL_2='02' then '父母'
when creq.EMER_REL_2='3' or creq.EMER_REL_2='03' then '子女'
when creq.EMER_REL_2='4' or creq.EMER_REL_2='04' then '亲戚'
when creq.EMER_REL_2='5' or creq.EMER_REL_2='05' then '朋友'
when creq.EMER_REL_2='6' or creq.EMER_REL_2='06' then '其他'
when creq.EMER_REL_2='7' or creq.EMER_REL_2='07' then '兄弟姐妹'
when creq.EMER_REL_2='8' or creq.EMER_REL_2='08' then '同事'
else '' end as `第二联系人关系`,
creq.EMER_PHONE_2 as `第二联系人联系电话`,
creq.EMER_NAME_3 as `第三联系人姓名`,
case when creq.EMER_REL_3='1' or creq.EMER_REL_3='01' then '配偶'
when creq.EMER_REL_3='2' or creq.EMER_REL_3='02' then '父母'
when creq.EMER_REL_3='3' or creq.EMER_REL_3='03' then '子女'
when creq.EMER_REL_3='4' or creq.EMER_REL_3='04' then '亲戚'
when creq.EMER_REL_3='5' or creq.EMER_REL_3='05' then '朋友'
when creq.EMER_REL_3='6' or creq.EMER_REL_3='06' then '其他'
when creq.EMER_REL_3='7' or creq.EMER_REL_3='07' then '兄弟姐妹'
when creq.EMER_REL_3='8' or creq.EMER_REL_3='08' then '同事'
else '' end as `第三联系人关系`,
creq.EMER_PHONE_3 as `第三联系人联系电话`,
creq.CREDIT_CONTRACT as `合同编号`,
trim(creq.CERT_NO) as `身份证号`,
case when creq.sex='2' then '男' when creq.sex='3' then '女' else '' end as `性别`,
loaninfo.limit_statr_date as `贷款开始日期`,
ureq.APP_START_DATE as `支用开始SAS日期`,
ureq.APP_END_DATE as `支用结束SAS日期`,
ureq.USE_DATE as `支用申请期限`,

merchant.contact_tel as `联系电话（APMS系统）`,
merchant.legal_name as `法定代表人姓名`,
merchant.finance_name as `财务联系人`,
merchant.finance_hp_no as `财务联系人电话（APMS系统）`,
merchant.stlm_acct as `还款银行卡号`,
merchant.name_busi as `商户名称`,
dict4.dict_name as `所在城市`,
merchant.busi_addr as `营业地址`,
dict1.dict_name as `分公司`,	
merchant.stlm_ins_city as `地市业务部`,
merchant.contact as `客户姓名`,
trim(creq.merchant_no) as `商户编号`,
dict2.dict_name as `inst_oid中文`,
dict3.dict_name as `mcc中文`,

trim(deductall.bill_no) as `借据编号`,
deductall.CURR_DATE as `截止日期`,

loaninfo.LOAN_AMT as `放款金额`,
loaninfo.LIMIT_END_DATE as `货款到期日`,
loaninfo.REMAIN_LOAN_AMT as `贷款剩余本金`,
loaninfo.REMAIN_LOAN_AMT as `应收未收本金`,
loaninfo.OVERDUE_INTE_AMT as `应收未收利息`,



case when compensate.BILL_NO is not null then '是'	else '否' end as `代偿状态`,
deductall.begin_late_date as `逾期开始日期`,
date_add(deductall.begin_late_date,1) as `新增日期`,
datediff(from_unixtime(unix_timestamp(deductall.CURR_DATE,'yyyymmdd'),'yyyy-mm-dd')
,deductall.begin_late_date) as `逾期天数`

from loginfo.union_flag1 deductall 
left join loginfo.t_loan_use_req  ureq 
on ureq.bill_no=trim(deductall.bill_no)
 left join loginfo.t_loan_credit_req creq on creq.LMT_SERNO=ureq.LMT_SERNO	
 left join loginfo.merchant_zc merchant on creq.merchant_no=merchant.mcht_cd
 left JOIN loginfo.T_REOCN_LOANINFO loaninfo on trim(deductall.bill_no)=loaninfo.bill_no and loaninfo.current_data=trim(deductall.curr_date)
left join loginfo.t_loan_compernsatory compensate on ureq.BILL_NO=compensate.BILL_NO
left join loginfo.t_allinpay_dict dict1 on merchant.aip_bran_id = dict1.dict_id and dict1.dict_type='aip_bran_id'
left join loginfo.t_allinpay_dict dict2 on merchant.inst_oid = dict2.dict_id and dict2.dict_type='inst_oid'
left join loginfo.t_allinpay_dict dict3 on merchant.up_mcc_cd = dict3.dict_id and dict3.dict_type='up_mcc_cd'
left join loginfo.t_allinpay_dict dict4 on merchant.city_cd = dict4.dict_id and dict4.dict_type='city_cd'
where creq.prod_code='8005000001' 
and from_unixtime(unix_timestamp(deductall.CURR_DATE,'yyyymmdd'))>=date_add(current_date(),-5)
    """
    print("OK" + "  " + sql2+"\n")
    cur.execute(sql2)  # , async=True)
    data = as_pandas(cur)

    print(len(data))

    name = '/home/data/deduct/deduct_gansu_late5_'+StrTool.get_the_date_str('',-1)+'.xlsx'

    writer = pd.ExcelWriter(name)

    data.to_excel(writer, 'Sheet1')

    writer.save()

    cur.close()
    conn.close()


if __name__ == "__main__":
    the_conf = ConfigData(p_is_test=False)

    run_hive(the_conf, the_date="")


