
# 清空数据

'''
----

从通金导入数据
+ tm_branch_info_20190121
+ tm_branch_info_statictis_20190121

----------------------------------------
tm_branch_info_20190121 -> th_branch_info_20190121,   							th_branch_info_20190121-> th_br_info
th_branch_info_20190121 +  branch_apms_bl_20190122 -> th_branch_info_20190122 , th_branch_info_20190122-> th_br_info
th_branch_info_20190122 +  branch_apms_bl_20190123 -> th_branch_info_20190123 , th_branch_info_20190123-> th_br_info
th_branch_info_20190123 +  branch_apms_bl_20190124 -> th_branch_info_20190124 , th_branch_info_20190124-> th_br_info
th_branch_info_20190124 +  branch_apms_bl_20190125 -> th_branch_info_20190125 , th_branch_info_20190125-> th_br_info
---
'''


def main_ok():
    sss = '''
select
  concat(mchtcd,'-',branchcd) as mcht_branch,
  opcode,mchtcd as mcht_cd,branchcd as branch_cd,
  apprdate as appr_date,deletedate as delete_date,
  aipbrancd as aip_bran_cd,busiarea as busi_area,
  account as account_name,branchbusinessstatus as branch_business_status
from
  rds_posflow.branch_apms_bl
where
cast(file_date as INT) = 20190122
'''

    sss = '''
select concat(mcht_cd,'-',branch_cd)  from rds_posflow.th_branch_info_20190121
'''
# ##############################################
# 代码方式字符控制

    p_sql_s = """
CREATE TABLE if not exists  rds_posflow.{}
( mcht_branch_id	string,
  mcht_cd string,
  branch_cd	string,
  appr_date	string,
  delete_date	string,
  aip_bran_cd	string,
  busi_area	string,
  account	string,
  account_name	string,
  branch_business_status	string
) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
  'separatorChar' = ',',
  'quoteChar' = '"',
  'escapeChar' = '\\\\'
)
STORED AS TEXTFILE""".format(111)

# HUE Query里的字符方式
    """
CREATE TABLE if not exists  rds_posflow.th_branch_info_20190126
( mcht_branch_id	string,
  mcht_cd	string,
  branch_cd	string,
  appr_date	string,
  delete_date	string,
  aip_bran_cd	string,
  busi_area	string,
  account	string,
  account_name	string,
  branch_business_status	string
) CLUSTERED BY (mcht_cd) into 20 BUCKETS
 ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
  'separatorChar' = ',',
  'quoteChar' = '\"',
  'escapeChar' = '\\'
)
STORED AS TEXTFILE
"""

    p_sql_s = """
set hive.enforce.bucketing = true;
set mapreduce.job.reduces=20;
"""
    p_sql_s = p_sql_s + """
insert into rds_posflow.th_branch_info_20190126
select 
  if( s.mcht_cd is null and s.branch_cd is null, d.mcht_branch_id, s.mcht_branch_id ) as mcht_branch_id,
  if( s.mcht_cd is null and s.branch_cd is null, d.mcht_cd, s.mcht_cd ) as mcht_cd,
  if( s.mcht_cd is null and s.branch_cd is null, d.branch_cd, s.branch_cd ) as branch_cd,
  if( s.mcht_cd is null and s.branch_cd is null, d.appr_date, s.appr_date ) as appr_date,
  if( s.mcht_cd is null and s.branch_cd is null, d.delete_date, s.delete_date ) as delete_date,
  if( s.mcht_cd is null and s.branch_cd is null, d.aip_bran_cd, s.aip_bran_cd ) as aip_bran_cd,
  if( s.mcht_cd is null and s.branch_cd is null, d.busi_area, s.busi_area ) as busi_area,
  if( s.mcht_cd is null and s.branch_cd is null, d.account, s.account ) as account,
  if( s.mcht_cd is null and s.branch_cd is null, d.account_name, s.account_name ) as account_name,
  if( s.mcht_cd is null and s.branch_cd is null, d.branch_business_status, s.branch_business_status ) as branch_business_status
from (
  select
    concat( mchtcd ,'-',branchcd) as mcht_branch_id ,
    mchtcd as mcht_cd,
    branchcd as branch_cd,
    apprdate as appr_date,
    deletedate as delete_date,
    aipbrancd as aip_bran_cd,
    busiarea as busi_area,
    account,accountname as account_name,
    branchbusinessstatus as branch_business_status
  from rds_posflow.branch_apms_bl
  where
    cast(file_date as INT) = 20190126 CLUSTER by( mcht_cd)
     )
  s full outer join ( 
    select
     concat( mcht_cd ,'-',branch_cd) as mcht_branch_id ,
     mcht_cd,branch_cd, appr_date, delete_date,
     aip_bran_cd, busi_area, account, account_name,
     branch_business_status
    from rds_posflow.th_branch_info_20190125 
    CLUSTER by( mcht_cd)
    )
  d on s.mcht_branch_id = d.mcht_branch_id 
  CLUSTER by( mcht_cd);
"""
#
#
#
#
#
#
#

# ####  New    OLD
# 21号   无   1774259

# 22号	New	    OLD      DISTINCT OLD    NEW  UPDATE  ALL
# 22好	1774300	1784709  1774259         847  9644    10491


# ##############################################

"""
SELECT count(*),opcode from
  rds_posflow.branch_apms_bl
where
cast(file_date as INT) = 20190122 GROUP BY opcode
"""
# NEW	UPDATE	ALL
# 847	9644	10491

# ##############################################
"""
SELECT count(DISTINCT(mcht_branch_id)) from th_branch_info_20190121
-- 1774259
"""
"""
SELECT count(DISTINCT(mcht_branch_id)) from th_branch_info_20190122_old
-- 1774259
"""
# -- 这个1784709的 distinct 是 1774259 个
#  1774259 of 1784709  DISTINCT OLD
# ##############################################
#
#
#
#
#
#
#
#
# ##############################################
'''
select mcht_branch_id 
from
  ( SELECT concat(mchtcd,'-',branchcd) as mcht_branch_id  from rds_posflow.branch_apms_bl where cast(file_date as INT) = 20190122 )
   a -- 这个是 10491 个
left semi join
  rds_posflow.th_branch_info_20190122_old b -- 这个1784709的 distinct 是 1774259 个
on a.mcht_branch_id = b.mcht_branch_id
'''
# 10450,  10491个数据不属于1774259中的，有10450个
# ##############################################
#
#
#
#
#
#
#
#
#
# ##############################################
'''
select d.mcht_branch_id , c.mcht_branch_id 
from
  ( SELECT concat(mchtcd,'-',branchcd) as mcht_branch_id  from rds_posflow.branch_apms_bl where cast(file_date as INT) = 20190122 ) 
  d  -- d有10491个
left outer join 
  (
    select mcht_branch_id
    from
      ( SELECT concat(mchtcd,'-',branchcd) as mcht_branch_id  from rds_posflow.branch_apms_bl where cast(file_date as INT) = 20190122 )
      a
    left semi join
      rds_posflow.th_branch_info_20190122_old b
    on a.mcht_branch_id = b.mcht_branch_id
  ) c -- C有10450个
on d.mcht_branch_id = c.mcht_branch_id
where c.mcht_branch_id is null -- 不加where 就是全部 10491 个，加就是41个
order by c.mcht_branch_id , d.mcht_branch_id
'''
# 有41个不在1774259中的
'''d.mcht_branch_id	c.mcht_branch_id
1	821140159122012-591220120199	NULL UPDATE
2	821140159122012-591220120200	NULL UPDATE
3	821140559330001-593300010075	NULL UPDATE
4	821232775310002-753100030000	NULL UPDATE

下面这些都是 UPDATE
5	821340415200856-152008990000	NULL
6	821340415200857-152009000000	NULL
7	821340415200858-152009010000	NULL
8	821340415200859-152009020000	NULL
9	821340415200860-152009030000	NULL
10  821340415200861-152009040000	NULL
11	821340415200862-152009050000	NULL
12	821340415200863-152009060000	NULL
13	821340415200864-152009070000	NULL
14	821340415200865-152009080000	NULL
15	821340415200866-152009090000	NULL
16	821340415200867-152009100000	NULL

下面这些都是 UPDATE
17	821350260124925-601202150000	NULL
18	821350260124932-601202190000	NULL
19	821350260124970-601202180000	NULL
20	821350260124994-601202160000	NULL
21	821350260124996-601202170000	NULL
22	821350855410046-554100490001	NULL
23	821360955110031-551100820000	NULL
24	821370170110261-701102900000	NULL
25	821371653990475-539905160000	NULL
26  821371653990476-539905170000	NULL
27	821393057220951-572285080000	NULL
28	821410182110003-821100030051	NULL
29	821441315200735-152006260000	NULL
30	821441315200736-152006270000	NULL
31	821445315200142-152001340000	NULL
32	821522259120000-591210030090	NULL
33	821610360510250-605102570000	NULL
34	821610360510251-605102580000	NULL
35	821610460510132-605101340000	NULL
36	821610660510137-605101380000	NULL
37	821610860510224-605102240000	NULL
38	821610860510225-605102250000	NULL
39	821610960510108-605101070000	NULL
40	821620154110001-541100340159	NULL
41	821630159980053-599800550000	NULL
'''

# 抽查从41个中抽，因为只增加了41个，看看他们在新旧表中是否有，旧表没有，
# 1	821140159122012-591220120199	NULL UPDATE   th_branch_info_20190122_old 无，
# 2	821140159122012-591220120200	NULL UPDATE   th_branch_info_20190122_old 无，
# 3	821140559330001-593300010075	NULL UPDATE   th_branch_info_20190122_old 无，
# 这3个是在10491个中的10450中的抽样，不在1774259中的，不在41个中

'''
SELECT * from th_branch_info_20190121 where
   ( mcht_cd = '821140159122012' and branch_cd = '591220120199' ) or
   ( mcht_cd = '821140159122012' and branch_cd = '591220120200' ) or
   ( mcht_cd = '821140559330001' and branch_cd = '593300010075' )
-- # 0 results.
'''
'''
SELECT * from th_branch_info_20190122_old where
   ( mcht_cd = '821140159122012' and branch_cd = '591220120199' ) or
   ( mcht_cd = '821140159122012' and branch_cd = '591220120200' ) or
   ( mcht_cd = '821140559330001' and branch_cd = '593300010075' )
-- # 0 results.
'''
'''
SELECT * from th_branch_info_20190122 where
   mcht_branch_id = '821140159122012-591220120199' or
   mcht_branch_id = '821140159122012-591220120200' or
   mcht_branch_id = '821140559330001-593300010075'
'''
# 821140559330001-593300010075	821140559330001	593300010075	2019-01-22		99991600	100			01
# 821140159122012-591220120200	821140159122012	591220120200	2019-01-22		99991600	100			01
# 821140159122012-591220120199	821140159122012	591220120199	2019-01-22		99991600	100			01


'''
SELECT * from  branch_apms_bl where 
( ( mchtcd = '821140159122012' and branchcd = '591220120199' ) or
  ( mchtcd = '821140159122012' and branchcd = '591220120200' ) or
  ( mchtcd = '821140559330001' and branchcd = '593300010075' )
 ) and file_date = '20190122'
'''
# 20190122	UPDATE	821140559330001	48211600	99991600	593300010075	晋城银行下村小城E站	14	05	25	下村镇下村村	李鹏	13363562623			2019-01-22		100	0_商业区	0_城区	08:00	20:00					01
# 20190122	UPDATE	821140159122012	48211600	99991600	591220120199	荣华大药房清徐人民医院店	14	01	21	康乐街169号商铺4-7号	张晋峰	15713503526			2019-01-22		100	0_商业区	0_城区	08:00	22:00					01
# 20190122	UPDATE	821140159122012	48211600	99991600	591220120200	荣华大药房迎泽西大街店	14	01	09	闫家沟社区24号楼一层2号商铺	张晋峰	15713503826			2019-01-22		100	0_商业区	0_城区	08:00	22:00					01
# ############################################
#
#
#
#
#
#
#
# ############################################
# 抽查，
# 这3个是在10450个中，不在上面41个新增的当中，说明旧表应该也有，看看是不是更新了。
#
# 文件里的内容
# "NEW","821620959980014","48218200","99998200","599800140000","酒泉思创会议会展有限责任公司"
# "UPDATE","821620959981263","48218200","99998200","599812630000","肃州区福华批发市场个体张晓莉"
# "UPDATE","821621186510000","48218200","99998200","865100000000","定西高速公路收费管理所"
#  这3个是在10450个中，在1774259中的，不在上面41中，这些新旧表都有，看看他们的数据和谁一样。

'''
SELECT * from  branch_apms_bl where 
( ( mchtcd = '821620959980014' and branchcd = '599800140000' ) or
  ( mchtcd = '821620959981263' and branchcd = '599812630000' ) or
  ( mchtcd = '821621186510000' and branchcd = '865100000000' )
 ) and file_date = '20190122'
'''
# 20190122	NEW  	821620959980014	48218200	99998200	599800140000	酒泉思创会议会展有限责任公司	62	09	02	肃园街14号	张洁	13014198387			2013-12-05		50	0_商业区	0_城区	08:00	22:00					01
# 20190122	UPDATE	821620959981263	48218200	99998200	599812630000	肃州区福华批发市场个体张晓莉	62	09	02	肃州区福华批发市场	张晓莉	13893781106			2015-07-06	2019-01-22	50	0_商业区	0_城区	08:00	22:00					02
# 20190122	UPDATE	821621186510000	48218200	99998200	865100000000	定西高速公路收费管理所	62	11	02	立交桥上定西收费站南侧	张小兵	13993208515			2014-06-06		50	0_商业区	0_城区	08:00	22:00					01
'''
SELECT * from th_branch_info_20190122 where
   mcht_branch_id = '821620959980014-599800140000' or
   mcht_branch_id = '821620959981263-599812630000' or
   mcht_branch_id = '821621186510000-865100000000'
'''
# 821621186510000-865100000000	821621186510000	865100000000	2014-06-06		99998200	50			01
# 821620959981263-599812630000	821620959981263	599812630000	2015-07-06	2019-01-22	99998200	50			02
# 821620959980014-599800140000	821620959980014	599800140000	2013-12-05		99998200	50			01

'''
SELECT * from th_branch_info_20190122_old where
   mcht_branch_id = '821620959980014-599800140000' or
   mcht_branch_id = '821620959981263-599812630000' or
   mcht_branch_id = '821621186510000-865100000000'
'''
# 821621186510000-865100000000	821621186510000	865100000000	2014-06-06		99998200	50			01
# 821620959981263-599812630000	821620959981263	599812630000	2015-07-06		99998200	50			01
# 821620959980014-599800140000	821620959980014	599800140000	2013-12-05		99998200	50			01
# 821620959980014-599800140000	821620959980014	599800140000	2013-12-05		99998200	50			01
# 821621186510000-865100000000	821621186510000	865100000000	2014-06-06		99998200	50			01
# 821620959981263-599812630000	821620959981263	599812630000	2015-07-06	2019-01-22	99998200	50			02
'''
SELECT * from th_branch_info_20190121 where
   ( mcht_cd = '821620959980014' and branch_cd = '599800140000' ) or
   ( mcht_cd = '821620959981263' and branch_cd = '599812630000' ) or
   ( mcht_cd = '821621186510000' and branch_cd = '865100000000' )
-- # 0 results.
'''
# 4651707	821620959980014	599800140000	2013-12-05		99998200	50			01
# 4652676	821620959981263	599812630000	2015-07-06		99998200	50			01
# 4656277	821621186510000	865100000000	2014-06-06		99998200	50			01
# ############################################
