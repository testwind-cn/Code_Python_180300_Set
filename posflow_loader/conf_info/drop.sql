alter table rds_posflow.t1_trxrecprd_v2_02  DROP IF EXISTS PARTITION( p_date>=20191215 )


alter table rds_posflow.t1_trxrecprd_v2_01  DROP IF EXISTS PARTITION( p_date>=20191215 )