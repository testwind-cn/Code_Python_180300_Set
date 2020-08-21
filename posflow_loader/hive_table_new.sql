-- 2020-06-22

-- 收银宝

CREATE TABLE IF NOT EXISTS `rds_posflow.t1_trxrecprd_v2`(
  `p_branch` string, 
  `sn` string, 
  `psn` string, 
  `mcht_cd` string, 
  `ssfgs` string, 
  `tzjg` string, 
  `whjg` string, 
  `md_id` string, 
  `term_id` string, 
  `trans_date` string, 
  `inst_date` string, 
  `product_name` string, 
  `trans_type` string, 
  `trans_status` string, 
  `trans_card` string, 
  `card_type` string, 
  `card_org` string, 
  `txn_amt` string, 
  `trans_amt` string, 
  `jfzq` string, 
  `fee_status` string, 
  `fee` string, 
  `cost` string, 
  `service_cost` string, 
  `income` string, 
  `mcc18` string, 
  `mcc42` string, 
  `zhhqfsbs` string, 
  `ywlx` string, 
  `bill_no` string, 
  `dfsh_no` string, 
  `dfzh_no` string, 
  `sstj` string, 
  `zdsqm` string, 
  `zdpch` string, 
  `zdgzh` string, 
  `jyck_no` string, 
  `qd_no` string, 
  `dyqd_mcht_no` string, 
  `trans_remark` string, 
  `trans_zy` string, 
  `trans_ip` string, 
  `res_code` string, 
  `commit_time` string, 
  `error_code` string, 
  `error_reason` string, 
  `qdjy_type` string, 
  `col_01` string, 
  `col_02` string, 
  `col_03` string, 
  `col_04` string, 
  `col_05` string, 
  `col_06` string, 
  `p_end` int)
PARTITIONED BY ( 
  `p_date` date COMMENT '日期分区')
STORED AS ORC;





-- 资产流水
DROP TABLE IF EXISTS rds_posflow.loginfo_rsp_zc;
DROP TABLE IF EXISTS rds_posflow.loginfo_rsp_agt_zc;

-- CREATE TABLE IF NOT EXISTS rds_posflow.loginfo_rsp_zc (
CREATE TABLE IF NOT EXISTS rds_posflow.loginfo_rsp_agt_zc (
    sn                  string,
    inst_date           string,
    inst_time           string,
    mcht_cd             string,
    term_id             string,
    card_type           string,
    currcy_code_trans   string,
    pan1                string,
    txn_amt             string,
    trans_amt           string
)
PARTITIONED BY (
    p_date date COMMENT '日期分区'
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE;





-- 资产风险
DROP TABLE IF EXISTS rds_posflow.rxinfo_rsp_zc;
DROP TABLE IF EXISTS rds_posflow.rxinfo_rsp_agt_zc;

-- CREATE TABLE IF NOT EXISTS rds_posflow.rxinfo_rsp_zc (
CREATE TABLE IF NOT EXISTS rds_posflow.rxinfo_rsp_agt_zc (
    rx_sn       string,
    inst_date   string,
    mcht_cd     string,
    rx_code     string,
    rx_type     string,
    rx_info1    string,
    rx_info2    string,
    rx_info3    string,
    rx_info4    string,
    rx_info5    string
)
PARTITIONED BY (
    p_date INT COMMENT '日期分区'
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE;




-- 保理流水
DROP TABLE IF EXISTS rds_posflow.loginfo_rsp_agt_bl;
DROP TABLE IF EXISTS rds_posflow.loginfo_rsp_bl;

-- CREATE TABLE IF NOT EXISTS rds_posflow.loginfo_rsp_bl (
CREATE TABLE IF NOT EXISTS rds_posflow.loginfo_rsp_agt_bl (
    sn              string,
    tx_date         string,
    tx_time         string,
    settle_time     string,
    mcht_cd         string,
    mcht_type       string,
    device_id       string,
    card_type       string,
    currency        string,
    card_no         string,
    card_bank       string,
    trans_type      string,
    trans_amt       string,
    settle_amt      string,
    fee             string,
    trans_status    string,
    card_inst       string,
    trans_area      string,
    origin_time     string
)
PARTITIONED BY (
    p_date date COMMENT '日期分区'
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE;





-- 保理风险
DROP TABLE IF EXISTS rds_posflow.rxinfo_rsp_agt_bl;

CREATE TABLE IF NOT EXISTS rds_posflow.rxinfo_rsp_bl (
    rx_sn       string,
    inst_date   string,
    mcht_cd     string,
    rx_code     string,
    rx_type     string,
    rx_info1    string,
    rx_info2    string,
    rx_info3    string,
    rx_info4    string,
    rx_info5    string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE;
