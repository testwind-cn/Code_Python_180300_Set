#!/bin/bash
# mirror -P=10 /9999110000/201811/08/ /home/data1 --log=./ftplog/lftp_log_${DATES}.txt

DATES=$(date -d "-0 day" +%Y%m%d%H%M%S)
# lftp -u thbl:ThBl2016 172.16.138.93 <<EOF
lftp -u thzc,'Th,12019!Zc' 172.16.138.93 <<EOF
mirror -P=10 / /home/data/SYB/ --log=./ftplog/lftp_log_${DATES}.txt
close
bye
EOF
