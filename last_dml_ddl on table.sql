select * from vid_ods.vid_sql_log;

select object_name, created, last_ddl_time from user_objects where object_name = 'vid_SQL_LOG';

OBJECT_NAME		CREATED					LAST_DDL_TIME
-----------------------------------------------------------
vid_SQL_LOG		5/18/2017 1:34:06 PM	7/17/2017 7:05:46 AM


select max(ora_rowscn), scn_to_timestamp(max(ora_rowscn)) from vid_ods.vid_sql_log;

MAX(ORA_ROWSCN)	SCN_TO_TIMESTAMP(MAX(ORA_ROWSCN))
-----------------------------------------------------
3984154445084	7/19/2017 12:16:22.000000000 PM


select ora_rowscn, scn_to_timestamp(ora_rowscn) from vid_ods.vid_sql_log;
ORA_ROWSCN		SCN_TO_TIMESTAMP(ORA_ROWSCN)
-----------------------------------------------------
3984154445084	7/19/2017 12:16:22.000000000 PM
3984154445084	7/19/2017 12:16:22.000000000 PM
3984154445084	7/19/2017 12:16:22.000000000 PM
3984154445084	7/19/2017 12:16:22.000000000 PM
3984154445084	7/19/2017 12:16:22.000000000 PM
3983695065525	7/17/2017 7:05:44.000000000 AM
