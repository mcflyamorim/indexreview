
-- Fabiano Amorim
-- http:\\www.blogfabiano.com | fabianonevesamorim@hotmail.com
SET NOCOUNT ON;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
SET LOCK_TIMEOUT 1000; /*1 second*/

/*
--Extended an DBA_CaptureStatsInfo event
--Create the DBA_CaptureStatsInfo xEvent using the following script:

USE master
GO
DECLARE @ErrMsg VarChar(8000)

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
  and please make sure you've at least 20GB available, or reduce/increase max_file_size 
  property if you want to change it.
*/

CREATE EVENT SESSION [DBA_CaptureStatsInfo] ON SERVER 
ADD EVENT sqlserver.auto_stats(
    WHERE ([package0].[not_equal_uint64]([database_id],(2))
           --AND [duration]>(0)
           ))
ADD TARGET package0.event_file(SET filename=N'F:\MSSQL\Traces\FabianoAmorim\xEvent\DBA_CaptureStatsInfo.xel',
                                    max_file_size=(1024)/*20GB*/,
                                    max_rollover_files=(20))
WITH (MAX_MEMORY=8192 KB,EVENT_RETENTION_MODE=ALLOW_SINGLE_EVENT_LOSS,
      MAX_DISPATCH_LATENCY=30 SECONDS,MAX_EVENT_SIZE=0 KB,
      MEMORY_PARTITION_MODE=NONE,TRACK_CAUSALITY=OFF,STARTUP_STATE=OFF);

/* Starting the event */
ALTER EVENT SESSION [DBA_CaptureStatsInfo]
ON SERVER STATE = START;

/* Stop the event */
/*
ALTER EVENT SESSION [DBA_CaptureStatsInfo]
ON SERVER STATE = STOP;
*/

*/
-- Read data from xEvent file
DECLARE @FileTargetPath	VarChar(800)	

SET @FileTargetPath = 'C:\temp\DBA_CaptureStatsInfo*.xel'

--SELECT * FROM sys.fn_xe_file_target_read_file(@FileTargetPath, default, null, null) AS tr

IF OBJECT_ID('tempdb..#tmp1xEvent') IS NOT NULL
  DROP TABLE #tmp1xEvent;

IF OBJECT_ID('tempdb..#tmp1xEvent') IS NULL
BEGIN
	 CREATE TABLE #tmp1xEvent
  (
   [object_name] VarChar(800),
	  event_data XML,
	  [file_name] VarChar(800)
  );
 	CREATE CLUSTERED INDEX IX_TimeStamp ON #tmp1xEvent ([object_name]);
END

INSERT INTO #tmp1xEvent
SELECT 
  [object_name],
  e.event_data.query('.'),
  @FileTargetPath AS [FileName]
FROM sys.fn_xe_file_target_read_file(@FileTargetPath, default, null, null) AS tr
CROSS APPLY (SELECT event_data = TRY_CONVERT(xml, event_data)) AS e

IF OBJECT_ID('tempdb..#tmp_auto_stats') IS NOT NULL
  DROP TABLE #tmp_auto_stats;

SELECT
  [object_name] AS [event_name],
  DATEADD(mi, DATEDIFF(mi, GETUTCDATE(), GETDATE()), event_data.value ('(event/@timestamp)[1]', 'datetime2')) AS timestamp_local,
  event_data.value ('(/event/action[@name=''session_id'']/value)[1]', 'BIGINT') AS session_id,
  event_data.value ('(/event/data[@name=''database_id'']/value)[1]', 'BIGINT') AS database_id,
  event_data.value ('(/event/data[@name=''database_name'']/value)[1]', 'VarChar(MAX)') AS database_name,
  event_data.value ('(/event/data[@name=''object_id'']/value)[1]', 'BIGINT') AS object_id,
  event_data.value ('(/event/data[@name=''index_id'']/value)[1]', 'BIGINT') AS index_id,
  event_data.value ('(/event/data[@name=''job_type'']/text)[1]', 'VarChar(MAX)') AS job_type,
  event_data.value ('(/event/data[@name=''status'']/text)[1]', 'VarChar(MAX)') AS status,
  event_data.value ('(/event/data[@name=''statistics_list'']/value)[1]', 'VarChar(MAX)') AS statistics_list,
  event_data.value ('(/event/data[@name=''duration'']/value)[1]', 'BIGINT') / 1000 AS duration_ms,
  event_data.value ('(/event/data[@name=''sample_percentage'']/value)[1]', 'BIGINT') AS sample_percentage,
  event_data.value ('(/event/data[@name=''max_dop'']/value)[1]', 'BIGINT') AS max_dop,
  event_data.value ('(/event/data[@name=''incremental'']/value)[1]', 'VarChar(100)') AS incremental,
  event_data.value ('(/event/data[@name=''async'']/value)[1]', 'VarChar(100)') AS async,
  event_data.value ('(/event/data[@name=''retries'']/value)[1]', 'BIGINT') AS retries,
  event_data.value ('(/event/data[@name=''success'']/value)[1]', 'VarChar(100)') AS success,
  event_data.value ('(/event/data[@name=''last_error'']/value)[1]', 'BIGINT') AS last_error,
  event_data.value ('(/event/data[@name=''count'']/value)[1]', 'BIGINT') AS count,
  event_data.value ('(/event/action[@name=''client_app_name'']/value)[1]', 'VarChar(MAX)') AS client_app_name,
  event_data.value ('(/event/action[@name=''client_hostname'']/value)[1]', 'VarChar(MAX)') AS client_hostname,
  event_data.value ('(/event/action[@name=''username'']/value)[1]', 'VarChar(MAX)') AS username,
  event_data.value ('(/event/action[@name=''sql_text'']/value)[1]', 'VarChar(MAX)') AS sql_text,
  event_data.query ('(/event/action[@name=''tsql_frame'']/value)[1]') AS tsql_frame
INTO #tmp_auto_stats
FROM #tmp1xEvent
WHERE [object_name] = 'auto_stats'

SELECT * 
FROM #tmp_auto_stats
ORDER BY timestamp_local desc

/*
IF OBJECT_ID('tempdb..#tmp_sp_statement_completed') IS NOT NULL
  DROP TABLE #tmp_sp_statement_completed;

SELECT
  [object_name] AS [event_name],
  DATEADD(mi, DATEDIFF(mi, GETUTCDATE(), GETDATE()), event_data.value ('(event/@timestamp)[1]', 'datetime2')) AS timestamp_local,
  event_data.value ('(/event/action[@name=''session_id'']/value)[1]', 'BIGINT') AS session_id,
  event_data.value ('(/event/action[@name=''database_id'']/value)[1]', 'BIGINT') AS database_id,
  event_data.value ('(/event/data[@name=''duration'']/value)[1]', 'BIGINT') / 1000 AS duration_ms,
  event_data.value ('(/event/data[@name=''cpu_time'']/value)[1]', 'BIGINT') AS cpu_time,
  event_data.value ('(/event/data[@name=''physical_reads'']/value)[1]', 'BIGINT') AS physical_reads,
  event_data.value ('(/event/data[@name=''logical_reads'']/value)[1]', 'BIGINT') AS logical_reads,
  event_data.value ('(/event/data[@name=''writes'']/value)[1]', 'BIGINT') AS writes,
  event_data.value ('(/event/data[@name=''row_count'']/value)[1]', 'BIGINT') AS row_count,
  event_data.value ('(/event/data[@name=''spills'']/value)[1]', 'BIGINT') AS spills,
  event_data.value ('(/event/data[@name=''statement'']/value)[1]', 'NVarChar(4000)') AS statement,
  event_data.value ('(/event/action[@name=''sql_text'']/value)[1]', 'VarChar(MAX)') AS sql_text,
  event_data.value ('(/event/data[@name=''object_id'']/value)[1]', 'BIGINT') AS object_id,
  event_data.value ('(/event/data[@name=''object_name'']/value)[1]', 'VarChar(MAX)') AS object_name,
  event_data.value ('(/event/data[@name=''object_type'']/text)[1]', 'VarChar(MAX)') AS object_type,
  event_data.value ('(/event/data[@name=''line_number'']/value)[1]', 'BIGINT') AS line_number,
  event_data.value ('(/event/data[@name=''offset'']/value)[1]', 'BIGINT') AS offset,
  event_data.value ('(/event/data[@name=''offset_end'']/value)[1]', 'BIGINT') AS offset_end,
  event_data.value ('(/event/action[@name=''client_app_name'']/value)[1]', 'VarChar(MAX)') AS client_app_name,
  event_data.value ('(/event/action[@name=''client_hostname'']/value)[1]', 'VarChar(MAX)') AS client_hostname,
  event_data.value ('(/event/action[@name=''username'']/value)[1]', 'VarChar(MAX)') AS username,
  event_data.query ('(/event/action[@name=''tsql_frame'']/value)[1]') AS tsql_frame
INTO #tmp_sp_statement_completed
FROM #tmp1xEvent
WHERE [object_name] = 'sp_statement_completed'


SELECT * FROM #tmp_sp_statement_completed
ORDER BY duration_ms desc
*/

GO
