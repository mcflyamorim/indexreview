USE master
GO
DECLARE @ErrMsg VARCHAR(8000)

DECLARE @sqlmajorver INT
SELECT @sqlmajorver = CONVERT(int, (@@microsoftversion / 0x1000000) & 0xff)

IF @sqlmajorver < 11 /*SQL2012*/
BEGIN
  SET @ErrMsg = 'Sorry, this requires at least SQL2012... It is about time to upgrade this old SQL version, pelase do it now.'
  RAISERROR (@ErrMsg, 16, -1) WITH NOWAIT
  RETURN
END

/* If event session already exists, then drop it. */
IF EXISTS (SELECT 1 FROM sys.server_event_sessions 
           WHERE name = 'DBA_CaptureStatsInfo')
BEGIN
  DROP EVENT SESSION [DBA_CaptureStatsInfo] ON SERVER;
END

/*
  Creating the event session
  Change filename entry if "'C:\Temp\DBA_CaptureStatsInfo.xel'" is not appropriate
  and please make sure you've at least 5GB available, or reduce/increase max_file_size 
  property if you want to change it.
*/

CREATE EVENT SESSION [DBA_CaptureStatsInfo] ON SERVER 
ADD EVENT sqlserver.auto_stats(SET collect_database_name=(1)
    ACTION(sqlserver.client_app_name,sqlserver.client_hostname,
           sqlserver.database_id,
           sqlserver.session_id,
           sqlserver.sql_text,
           sqlserver.tsql_frame,
           sqlserver.username)
    WHERE ([package0].[not_equal_uint64]([database_id],(2))
           AND 
           [duration]>(0))),
ADD EVENT sqlserver.sp_statement_completed(SET collect_object_name=(1),collect_statement=(1)
    ACTION(sqlserver.client_app_name,
           sqlserver.client_hostname,
           sqlserver.database_id,
           sqlserver.session_id,
           sqlserver.sql_text,
           sqlserver.tsql_frame,
           sqlserver.username)
    WHERE ([sqlserver].[like_i_sql_unicode_string]([statement],N'SELECT StatMan%'))
           AND 
          ([source_database_id]<>(2)))
ADD TARGET package0.event_file(SET filename=N'C:\temp\DBA_CaptureStatsInfo.xel',
                                    max_file_size=(5120),
                                    max_rollover_files=(0))
WITH (MAX_MEMORY=8192 KB,EVENT_RETENTION_MODE=ALLOW_SINGLE_EVENT_LOSS,
      MAX_DISPATCH_LATENCY=30 SECONDS,MAX_EVENT_SIZE=0 KB,
      MEMORY_PARTITION_MODE=NONE,TRACK_CAUSALITY=OFF,STARTUP_STATE=OFF);

/* Starting the event */
ALTER EVENT SESSION [DBA_CaptureStatsInfo]
ON SERVER STATE = START;
GO

/* Stop the event */
/*
ALTER EVENT SESSION [DBA_CaptureStatsInfo]
ON SERVER STATE = STOP;
GO
*/