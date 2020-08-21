CREATE TABLE IF NOT EXISTS `rds_posflow.loginfo_rsp_agt_bl_02`(
  `sn` string,
  `tx_date` string,
  `tx_time` string,
  `settle_time` string,
  `mcht_cd` string,
  `mcht_type` string,
  `device_id` string,
  `card_type` string,
  `currency` string,
  `card_no` string,
  `card_bank` string,
  `trans_type` string,
  `trans_amt` string,
  `settle_amt` string,
  `fee` string,
  `trans_status` string,
  `card_inst` string,
  `trans_area` string,
  `origin_time` string)
PARTITIONED BY (
    p_date date COMMENT '日期分区'
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE;