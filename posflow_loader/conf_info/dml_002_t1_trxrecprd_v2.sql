set mapreduce.map.memory.mb=4096;
set mapreduce.reduce.memory.mb=4096;
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
-- set mapreduce.job.reduces=1;

-- 设置 map输出和reduce输出进行合并的相关参数
set hive.merge.mapredfiles =true;
set hive.merge.mapfiles=true;
set hive.merge.size.per.task=512000000;
set hive.merge.smallfiles.avgsize=512000000;

-- 设置 mapper输入文件合并一些参数：
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set mapred.max.split.size=1024000000;
set mapred.min.split.size.per.node=768000000;
set mapred.min.split.size.per.rack=768000000;

INSERT OVERWRITE TABLE rds_posflow.t1_trxrecprd_v2
PARTITION(p_date)
SELECT
    p_branch        ,
    sn              ,
    psn             ,
    mcht_cd         ,
    ssfgs           ,
    tzjg            ,
    whjg            ,
    md_id           ,
    term_id         ,
    trans_date      ,
    inst_date       ,
    product_name    ,
    trans_type      ,
    trans_status    ,
    trans_card      ,
    card_type       ,
    card_org        ,
    txn_amt         ,
    trans_amt       ,
    jfzq            ,
    fee_status      ,
    fee             ,
    cost            ,
    service_cost    ,
    income          ,
    MCC18           ,
    MCC42           ,
    zhhqfsbs        ,
    ywlx            ,
    bill_no         ,
    dfsh_no         ,
    dfzh_no         ,
    sstj            ,
    zdsqm           ,
    zdpch           ,
    zdgzh           ,
    jyck_no         ,
    qd_no           ,
    dyqd_mcht_no    ,
    trans_remark    ,
    trans_zy        ,
    trans_ip        ,
    res_code        ,
    commit_time     ,
    error_code      ,
    error_reason    ,
    qdjy_type       ,
    col_01          ,
    col_02          ,
    col_03          ,
    col_04          ,
    col_05          ,
    col_06          ,
    p_end           ,
    p_date
from deprecated_db.t1_trxrecprd_v2_01
where
    p_date = date_add(current_date(),-1)
--    p_date >= to_date('2017-10-01') and  p_date < to_date('2017-11-01')
;


