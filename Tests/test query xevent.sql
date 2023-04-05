
DECLARE @FileTargetPath	VARCHAR(800)	

SET @FileTargetPath = 'C:\temp\DBA_CaptureStatsInfo*.xel'

--SELECT * FROM sys.fn_xe_file_target_read_file(@FileTargetPath, default, null, null) AS tr

IF OBJECT_ID('tempdb..#tmp1') IS NOT NULL
  DROP TABLE #tmp1;

IF OBJECT_ID('tempdb..#tmp1') IS NULL
BEGIN
	 CREATE TABLE #tmp1
  (
	  timestamp_local DATETIME2,
   [object_name] VARCHAR(800),
	  event_data XML,
	  [file_name] VARCHAR(800)
  );
 	CREATE CLUSTERED INDEX IX_TimeStamp ON #tmp1 ([object_name], timestamp_local ASC);
END

INSERT INTO #tmp1
SELECT 
  DATEADD(mi, DATEDIFF(mi, GETUTCDATE(), GETDATE()), timestamp_utc) AS timestamp_local,
  [object_name],
  e.event_data.query('.'),
  @FileTargetPath AS [FileName]
FROM sys.fn_xe_file_target_read_file(@FileTargetPath, default, null, null) AS tr
CROSS APPLY (SELECT event_data = TRY_CONVERT(xml, event_data)) AS e

-- SELECT * FROM #tmp1

IF OBJECT_ID('tempdb..#tmp_sp_statement_completed') IS NOT NULL
  DROP TABLE #tmp_sp_statement_completed;

SELECT
  [object_name] AS [event_name],
  timestamp_local,
  event_data.value ('(/event/action[@name=''session_id'']/value)[1]', 'BIGINT') AS session_id,
  event_data.value ('(/event/action[@name=''database_id'']/value)[1]', 'BIGINT') AS database_id,
  event_data.value ('(/event/data[@name=''duration'']/value)[1]', 'BIGINT') / 1000 AS duration_ms,
  event_data.value ('(/event/data[@name=''cpu_time'']/value)[1]', 'BIGINT') AS cpu_time,
  event_data.value ('(/event/data[@name=''physical_reads'']/value)[1]', 'BIGINT') AS physical_reads,
  event_data.value ('(/event/data[@name=''logical_reads'']/value)[1]', 'BIGINT') AS logical_reads,
  event_data.value ('(/event/data[@name=''writes'']/value)[1]', 'BIGINT') AS writes,
  event_data.value ('(/event/data[@name=''row_count'']/value)[1]', 'BIGINT') AS row_count,
  event_data.value ('(/event/data[@name=''spills'']/value)[1]', 'BIGINT') AS spills,
  event_data.value ('(/event/data[@name=''statement'']/value)[1]', 'NVARCHAR(4000)') AS statement,
  event_data.value ('(/event/action[@name=''sql_text'']/value)[1]', 'VARCHAR(MAX)') AS sql_text,
  event_data.value ('(/event/data[@name=''object_id'']/value)[1]', 'BIGINT') AS object_id,
  event_data.value ('(/event/data[@name=''object_name'']/value)[1]', 'VARCHAR(MAX)') AS object_name,
  event_data.value ('(/event/data[@name=''object_type'']/text)[1]', 'VARCHAR(MAX)') AS object_type,
  event_data.value ('(/event/data[@name=''line_number'']/value)[1]', 'BIGINT') AS line_number,
  event_data.value ('(/event/data[@name=''offset'']/value)[1]', 'BIGINT') AS offset,
  event_data.value ('(/event/data[@name=''offset_end'']/value)[1]', 'BIGINT') AS offset_end,
  event_data.value ('(/event/action[@name=''client_app_name'']/value)[1]', 'VARCHAR(MAX)') AS client_app_name,
  event_data.value ('(/event/action[@name=''client_hostname'']/value)[1]', 'VARCHAR(MAX)') AS client_hostname,
  event_data.value ('(/event/action[@name=''username'']/value)[1]', 'VARCHAR(MAX)') AS username,
  event_data.query ('(/event/action[@name=''tsql_frame'']/value)[1]') AS tsql_frame
INTO #tmp_sp_statement_completed
FROM #tmp1
WHERE [object_name] = 'sp_statement_completed'


SELECT * FROM #tmp_sp_statement_completed
ORDER BY duration_ms desc


IF OBJECT_ID('tempdb..#tmp_auto_stats') IS NOT NULL
  DROP TABLE #tmp_auto_stats;

SELECT
  [object_name] AS [event_name],
  timestamp_local,
  event_data.value ('(/event/action[@name=''session_id'']/value)[1]', 'BIGINT') AS session_id,
  event_data.value ('(/event/data[@name=''database_id'']/value)[1]', 'BIGINT') AS database_id,
  event_data.value ('(/event/data[@name=''database_name'']/value)[1]', 'VARCHAR(MAX)') AS database_name,
  event_data.value ('(/event/data[@name=''index_id'']/value)[1]', 'BIGINT') AS index_id,
  event_data.value ('(/event/data[@name=''job_type'']/text)[1]', 'VARCHAR(MAX)') AS job_type,
  event_data.value ('(/event/data[@name=''status'']/text)[1]', 'VARCHAR(MAX)') AS status,
  event_data.value ('(/event/data[@name=''statistics_list'']/value)[1]', 'VARCHAR(MAX)') AS statistics_list,
  event_data.value ('(/event/data[@name=''duration'']/value)[1]', 'BIGINT') / 1000 AS duration_ms,
  event_data.value ('(/event/data[@name=''sample_percentage'']/value)[1]', 'BIGINT') AS sample_percentage,
  event_data.value ('(/event/data[@name=''max_dop'']/value)[1]', 'BIGINT') AS max_dop,
  event_data.value ('(/event/data[@name=''incremental'']/value)[1]', 'VARCHAR(100)') AS incremental,
  event_data.value ('(/event/data[@name=''async'']/value)[1]', 'VARCHAR(100)') AS async,
  event_data.value ('(/event/data[@name=''retries'']/value)[1]', 'BIGINT') AS retries,
  event_data.value ('(/event/data[@name=''success'']/value)[1]', 'VARCHAR(100)') AS success,
  event_data.value ('(/event/data[@name=''last_error'']/value)[1]', 'BIGINT') AS last_error,
  event_data.value ('(/event/data[@name=''count'']/value)[1]', 'BIGINT') AS count,
  event_data.value ('(/event/action[@name=''client_app_name'']/value)[1]', 'VARCHAR(MAX)') AS client_app_name,
  event_data.value ('(/event/action[@name=''client_hostname'']/value)[1]', 'VARCHAR(MAX)') AS client_hostname,
  event_data.value ('(/event/action[@name=''username'']/value)[1]', 'VARCHAR(MAX)') AS username,
  event_data.value ('(/event/action[@name=''sql_text'']/value)[1]', 'VARCHAR(MAX)') AS sql_text,
  event_data.query ('(/event/action[@name=''tsql_frame'']/value)[1]') AS tsql_frame
INTO #tmp_auto_stats
FROM #tmp1
WHERE [object_name] = 'auto_stats'


SELECT * FROM #tmp_auto_stats
WHERE status <>  'Failed to save computed stats'
AND status <> 'Failed to obtain schema lock on stats'
ORDER BY duration_ms desc
GO

SELECT * FROM #tmp_sp_statement_completed
ORDER BY duration_ms desc
