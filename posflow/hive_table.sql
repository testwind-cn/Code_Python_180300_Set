
    drop table rds_posflow.loginfo_rsp_agt_bl;
    drop table rds_posflow.loginfo_rsp_bl;

    CREATE TABLE rds_posflow.loginfo_rsp_bl (
CREATE TABLE rds_posflow.loginfo_rsp_agt_bl (
sn     string,
tx_date     string,
tx_time     string,
settle_time     string,
mcht_cd     string,
mcht_type     string,
device_id     string,
card_type     string,
currency     string,
card_no     string,
card_bank     string,
trans_type     string,
trans_amt     string,
settle_amt     string,
fee     string,
trans_status     string,
card_inst     string,
trans_area     string,
origin_time     string
)ROW format delimited fields terminated BY ',' STORED AS TEXTFILE;

    drop table rds_posflow.rxinfo_rsp_agt_bl;

CREATE TABLE rds_posflow.rxinfo_rsp_bl (
    rx_sn     string     ,
    inst_date     string     ,
    mcht_cd     string     ,
    rx_code     string     ,
    rx_type     string     ,
    rx_info1     string     ,
    rx_info2     string     ,
    rx_info3     string     ,
    rx_info4     string     ,
    rx_info5     string
    )ROW format delimited fields terminated BY ',' STORED AS TEXTFILE;

    drop table rds_posflow.loginfo_rsp_agt_zc;
    drop table rds_posflow.loginfo_rsp_zc;

    CREATE TABLE rds_posflow.loginfo_rsp_zc (
CREATE TABLE rds_posflow.loginfo_rsp_agt_zc (
    sn     string     ,
    inst_date     string     ,
    inst_time     string     ,
    mcht_cd     string     ,
    term_id     string     ,
    card_type     string     ,
    currcy_code_trans     string     ,
    pan1     string     ,
    txn_amt     string     ,
    trans_amt     string
)ROW format delimited fields terminated BY ',' STORED AS TEXTFILE;

    drop table rds_posflow.rxinfo_rsp_zc;
    drop table rds_posflow.rxinfo_rsp_agt_zc;

    CREATE TABLE rds_posflow.rxinfo_rsp_zc (
CREATE TABLE rds_posflow.rxinfo_rsp_agt_zc (
    rx_sn     string     ,
    inst_date     string     ,
    mcht_cd     string     ,
    rx_code     string     ,
    rx_type     string     ,
    rx_info1     string     ,
    rx_info2     string     ,
    rx_info3     string     ,
    rx_info4     string     ,
    rx_info5     string
    )ROW format delimited fields terminated BY ',' STORED AS TEXTFILE;



    drop table rds_posflow.loginfo_rsp_bl;

    CREATE TABLE rds_posflow.loginfo_rsp_bl (
CREATE TABLE rds_posflow.loginfo_rsp_agt_bl (
sn     string,
tx_date     string,
tx_time     string,
settle_time     string,
mcht_cd     string,
mcht_type     string,
device_id     string,
card_type     string,
currency     string,
card_no     string,
card_bank     string,
trans_type     string,
trans_amt     string,
settle_amt     string,
fee     string,
trans_status     string,
card_inst     string,
trans_area     string,
origin_time     string
)ROW format delimited fields terminated BY ',' STORED AS TEXTFILE;

   mchtCd
UpBcCd
AipBranCd
branchCd
Name
ProvcCd
CityCd
AreaCd
Addr
Contact
Tel
Fax
Email
ApprDate
DeleteDate
BusiArea
ProdArea
ProdRegion
TimeOpen
TimeClose
Account
AccountName
BankCode
BankName
BranchBusinessStatus



     drop table rds_posflow.branch_apms_bl;

    CREATE TABLE rds_posflow.branch_apms_bl (
file_date		string,
opcode	string,
mchtcd	string,
upbccd	string,
aipbrancd	string,
branchcd	string,
name	string,
provccd	string,
citycd	string,
areacd	string,
addr	string,
contact	string,
tel	string,
fax	string,
email	string,
apprdate	string,
deletedate	string,
busiarea	string,
prodarea	string,
prodregion	string,
timeopen	string,
timeclose	string,
account	string,
accountname	string,
bankcode	string,
bankname	string,
branchbusinessstatus	string
) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    "separatorChar" = ",",
    "quoteChar" = "\"",
    "escapeChar" = "\\"
)
STORED AS TEXTFILE;


ROW format delimited fields terminated BY ',' STORED AS TEXTFILE;


hive> show create table rds_posflow.cal_branch_apms_bl
    > ;
OK
CREATE TABLE `rds_posflow.cal_branch_apms_bl`(
  `file_date` string COMMENT 'from deserializer',
  `mchtcd` string COMMENT 'from deserializer',
  `upbccd` string COMMENT 'from deserializer',
  `aipbrancd` string COMMENT 'from deserializer',
  `branchcd` string COMMENT 'from deserializer',
  `name` string COMMENT 'from deserializer',
  `provccd` string COMMENT 'from deserializer',
  `citycd` string COMMENT 'from deserializer',
  `areacd` string COMMENT 'from deserializer',
  `addr` string COMMENT 'from deserializer',
  `contact` string COMMENT 'from deserializer',
  `tel` string COMMENT 'from deserializer',
  `fax` string COMMENT 'from deserializer',
  `email` string COMMENT 'from deserializer',
  `apprdate` string COMMENT 'from deserializer',
  `deletedate` string COMMENT 'from deserializer',
  `busiarea` string COMMENT 'from deserializer',
  `prodarea` string COMMENT 'from deserializer',
  `prodregion` string COMMENT 'from deserializer',
  `timeopen` string COMMENT 'from deserializer',
  `timeclose` string COMMENT 'from deserializer',
  `account` string COMMENT 'from deserializer',
  `accountname` string COMMENT 'from deserializer',
  `bankcode` string COMMENT 'from deserializer',
  `bankname` string COMMENT 'from deserializer',
  `branchbusinessstatus` string COMMENT 'from deserializer')
ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
  'escapeChar'='\\',
  'quoteChar'='\"',
  'separatorChar'=',')
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'hdfs://nn1:8020/user/hive/warehouse/rds_posflow.db/cal_branch_apms_bl'
TBLPROPERTIES (
  'COLUMN_STATS_ACCURATE'='true',
  'numFiles'='600',
  'skip.header.line.count'='1',
  'totalSize'='257215375',
  'transient_lastDdlTime'='1547549230')
Time taken: 0.793 seconds, Fetched: 45 row(s)
hive>
    >
    > show create table rds_posflow.branch_apms_bl;
OK
CREATE TABLE `rds_posflow.branch_apms_bl`(
  `file_date` string COMMENT 'from deserializer',
  `opcode` string COMMENT 'from deserializer',
  `mchtcd` string COMMENT 'from deserializer',
  `upbccd` string COMMENT 'from deserializer',
  `aipbrancd` string COMMENT 'from deserializer',
  `branchcd` string COMMENT 'from deserializer',
  `name` string COMMENT 'from deserializer',
  `provccd` string COMMENT 'from deserializer',
  `citycd` string COMMENT 'from deserializer',
  `areacd` string COMMENT 'from deserializer',
  `addr` string COMMENT 'from deserializer',
  `contact` string COMMENT 'from deserializer',
  `tel` string COMMENT 'from deserializer',
  `fax` string COMMENT 'from deserializer',
  `email` string COMMENT 'from deserializer',
  `apprdate` string COMMENT 'from deserializer',
  `deletedate` string COMMENT 'from deserializer',
  `busiarea` string COMMENT 'from deserializer',
  `prodarea` string COMMENT 'from deserializer',
  `prodregion` string COMMENT 'from deserializer',
  `timeopen` string COMMENT 'from deserializer',
  `timeclose` string COMMENT 'from deserializer',
  `account` string COMMENT 'from deserializer',
  `accountname` string COMMENT 'from deserializer',
  `bankcode` string COMMENT 'from deserializer',
  `bankname` string COMMENT 'from deserializer',
  `branchbusinessstatus` string COMMENT 'from deserializer')
ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
  'escapeChar'='\\',
  'quoteChar'='\"',
  'separatorChar'=',')
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'hdfs://nn1:8020/user/hive/warehouse/rds_posflow.db/branch_apms_bl'
TBLPROPERTIES (
  'COLUMN_STATS_ACCURATE'='true',
  'numFiles'='922',
  'totalSize'='1027794337',
  'transient_lastDdlTime'='1547535529')
Time taken: 0.031 seconds, Fetched: 45 row(s)


