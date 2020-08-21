#!/bin/bash

THEDIR="$(dirname $0)/"
cd ${THEDIR}
pwd
echo "当前目录  ${THEDIR}"

TABLE_S=rds_posflow.t1_trxrecprd_v2_01

if [[ $# -ge 1 ]]
then
    DATE_L=$1
else
    DATE_L=$(date -d "-1 day" "+%Y%m%d")
fi


echo "开始处理SQL逻辑：${TABLE_S} 日期： ${DATE_L} ***********************"

# sudo -u admin hive --hivevar THE_TABLE=${TABLE_S} --hivevar THE_DATE=${DATE_L} -e "$(cat main_ddl_01.sql ; cat main_dml_01.sql)"
# sudo -u hdfs hive --hivevar THE_DATE=${DATE_L} --hivevar BACKUP_DATE=${DATE_BK} -f main_dml_02.sql

sudo -u admin hive -e "$(cat dml_002_t1_trxrecprd_v2.sql)"

echo "完成处理SQL逻辑：日期： ${DATE_L} ***********************"

