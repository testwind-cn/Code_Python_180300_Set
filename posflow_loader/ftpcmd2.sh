#!/bin/bash
# mirror -P=10 /9999110000/201811/08/ /home/data1 --log=./ftplog/lftp_log_${DATES}.txt

DATES=$(date -d "-0 day" +%Y%m%d%H%M%S)
lftp -u poslsyb,6yaKlBHvzzl0 sftp://172.31.71.71:12306 <<EOF
mirror -P=10 / /home/data/SYB_RISK/ --log=./ftplog/SYB_RISK/lftp_syb_risk_log_${DATES}.txt
close
bye
EOF
