USE [master];
GO

IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.ROUTINES WHERE ROUTINE_NAME = 'sp_GetIndexInfo')
	EXEC ('CREATE PROC dbo.sp_GetIndexInfo AS SELECT 1')
GO

ALTER PROC dbo.sp_GetIndexInfo
(
  @database_name_filter NVARCHAR(200) = NULL, /* By default I'm collecting information about all DBs */
  @refreshdata  BIT = 0, /* 1 to force drop/create of index tables, 0 will skip table creation if they already exists */
  @skipcache    BIT = 0, /* use 1 skip plan cache collection*/
  @skipfrag     BIT = 0  /* use 1 skip fragmentation collection*/
)
/*
-------------------------------------------------------------------------------
| .___            .___              __________            .__                 |
| |   | ____    __| _/____ ___  ___ \______   \ _______  _|__| ______  _  __  |
| |   |/    \  / __ |/ __ \\  \/  /  |       _// __ \  \/ /  |/ __ \ \/ \/ /  |
| |   |   |  \/ /_/ \  ___/ >    <   |    |   \  ___/\   /|  \  ___/\     /   |
| |___|___|  /\____ |\___  >__/\_ \  |____|_  /\___  >\_/ |__|\___  >\/\_/    |
|          \/      \/    \/      \/         \/     \/             \/          |
|                                           __                                |
|                                  |_     |__ |_ . _  _  _    /\  _  _  _. _  |
|                                  |_)\/  |(_||_)|(_|| )(_)  /--\|||(_)| |||| |
-------------------------------------------------------------------------------

sp_GetIndexInfo - March 2023 (v1)

Fabiano Amorim
http:\\www.blogfabiano.com | fabianonevesamorim@hotmail.com

For help and more information, visit https://github.com/mcflyamorim/IndexReview

How to use:
Collect statistic information for all DBs:
  EXEC sp_GetIndexInfo @database_name_filter = NULL

Collect statistic information for Northwind DB:
  EXEC sp_GetIndexInfo @database_name_filter = 'Northwind', @refreshdata = 1

Credit: 
Some checks and scripts were used based on 
Brent Ozar sp_blitz scripts, MS Tiger team BP, Glenn Berry's diagnostic queries, Kimberly Tripp queries
and probably a lot of other SQL community folks out there, so, a huge kudos for SQL community.

Important notes and pre-requisites:
 * Found a bug or want to change something? Please feel free to create an issue on https://github.com/mcflyamorim/IndexReview
   or, you can also e-mail (really? I didn't know people were still using this.) me at fabianonevesamorim@hotmail.com
 * Depending on the number of indexes, the PS script to generate the excel file may use a lot (a few GBs) of memory.

Known issues and limitations:
 * Not tested and not support on Azure SQL DBs, Amazon RDS and Managed Instances (I�m planning to add support for this in a new release).

Disclaimer:
This code and information are provided "AS IS" without warranty of any kind, either expressed or implied.
Furthermore, the author shall not be liable for any damages you may sustain by using this information, whether direct, 
indirect, special, incidental or consequential, even if it has been advised of the possibility of such damages.
	
License:
Pretty much free to everyone and to do anything you'd like as per MIT License - https://en.wikipedia.org/wiki/MIT_License

With all love and care,
Fabiano Amorim

*/

AS
BEGIN

SET NOCOUNT ON;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
SET LOCK_TIMEOUT 1000; /*1 second*/

DECLARE @statusMsg  VARCHAR(MAX) = ''

/* If data already exists, skip the population, unless refresh was asked via @refreshdata */
IF OBJECT_ID('tempdb.dbo.Tab_GetIndexInfo') IS NOT NULL
BEGIN
  /* 
     I'm assuming data for all tables exists, but I'm only checking tmp_stats... 
     if you're not sure if this is ok, use @refreshdata = 1 to force the refresh and 
     table population
  */
  IF EXISTS(SELECT 1 FROM tempdb.dbo.Tab_GetIndexInfo) AND (@refreshdata = 0)
  BEGIN
			 SELECT @statusMsg = '[' + CONVERT(NVARCHAR(200), GETDATE(), 120) + '] - ' + 'Table with list of indexes already exists, I''ll reuse it and skip the code to populate the table.'
    RAISERROR (@statusMsg, 0, 0) WITH NOWAIT
    RETURN
  END
  ELSE
  BEGIN
    DROP TABLE tempdb.dbo.Tab_GetIndexInfo
  END
END

IF @skipfrag = 1
BEGIN
  IF OBJECT_ID('tempdb.dbo.#tmp_skipfrag') IS NOT NULL
    DROP TABLE #tmp_skipfrag

  CREATE TABLE #tmp_skipfrag (ID INT)
END

/* Clean up tables from a old execution */
DECLARE @sql_old_table NVARCHAR(MAX)
DECLARE @tmp_table_name NVARCHAR(MAX)

IF OBJECT_ID('tempdb.dbo.#tmp_old_exec') IS NOT NULL
  DROP TABLE #tmp_old_exec

SELECT [name] 
INTO #tmp_old_exec
FROM tempdb.sys.tables
WHERE type = 'U'
AND name LIKE'tmpIndexCheck%'

DECLARE c_old_exec CURSOR read_only FOR
    SELECT [name] FROM #tmp_old_exec
OPEN c_old_exec

FETCH NEXT FROM c_old_exec
INTO @tmp_table_name
WHILE @@FETCH_STATUS = 0
BEGIN
  SET @sql_old_table = 'DROP TABLE tempdb.dbo.[' + @tmp_table_name + '];'; 
  EXEC (@sql_old_table)

  FETCH NEXT FROM c_old_exec
  INTO @tmp_table_name
END
CLOSE c_old_exec
DEALLOCATE c_old_exec

SET @statusMsg = '[' + CONVERT(VARCHAR(200), GETDATE(), 120) + '] - ' + 'Collecting BP usage info...'
RAISERROR(@statusMsg, 0, 42) WITH NOWAIT;

IF OBJECT_ID('tempdb.dbo.#tmpBufferDescriptors') IS NOT NULL
    DROP TABLE #tmpBufferDescriptors;

SELECT database_id,
       allocation_unit_id,
       CONVERT(DECIMAL(25, 2), (COUNT(*) * 8) / 1024.) AS CacheSizeMB,
       CONVERT(DECIMAL(25, 2), (SUM(CONVERT(NUMERIC(25,2), free_space_in_bytes)) / 1024.) / 1024.) AS FreeSpaceMB
INTO #tmpBufferDescriptors
FROM (SELECT * FROM sys.dm_os_buffer_descriptors) AS t
GROUP BY database_id, allocation_unit_id;

CREATE CLUSTERED INDEX ix1 ON #tmpBufferDescriptors (database_id, allocation_unit_id);

SET @statusMsg = '[' + CONVERT(VARCHAR(200), GETDATE(), 120) + '] - ' + 'Finished to collect BP usage info...'
RAISERROR(@statusMsg, 0, 42) WITH NOWAIT;

SET @statusMsg = '[' + CONVERT(VARCHAR(200), GETDATE(), 120) + '] - ' + 'Starting to collect cache plan info...'
RAISERROR(@statusMsg, 0, 42) WITH NOWAIT;

/* Config params: */
DECLARE @TOP BIGINT = 5000 /* By default, I'm only reading TOP 5k plans */

IF @skipcache = 1
BEGIN
  SET @TOP = 0
END

IF OBJECT_ID('tempdb.dbo.#tmpdm_exec_query_stats') IS NOT NULL
  DROP TABLE #tmpdm_exec_query_stats
  
DECLARE @total_elapsed_time BIGINT,
        @total_worker_time BIGINT,
        @total_logical_page_reads BIGINT,
        @total_physical_page_reads BIGINT,
        @total_logical_page_writes BIGINT,
        @total_execution_count BIGINT;

SELECT  @total_worker_time = SUM(total_worker_time),
        @total_elapsed_time = SUM(total_elapsed_time),
        @total_logical_page_reads = SUM(total_logical_reads),
        @total_physical_page_reads = SUM(total_physical_reads),
        @total_logical_page_writes = SUM(total_logical_writes),
        @total_execution_count = SUM(execution_count)
FROM sys.dm_exec_query_stats
WHERE dm_exec_query_stats.total_worker_time > 0 /* Only plans with CPU time > 0ms */
AND dm_exec_query_stats.query_plan_hash <> 0x0000000000000000
AND NOT EXISTS(SELECT 1 
                 FROM sys.dm_exec_cached_plans
                 WHERE dm_exec_cached_plans.plan_handle = dm_exec_query_stats.plan_handle
                 AND dm_exec_cached_plans.cacheobjtype = 'Compiled Plan Stub') /*Ignoring AdHoc - Plan Stub*/
AND @skipcache = 0
OPTION (RECOMPILE);

IF OBJECT_ID('tempdb.dbo.#tmpdm_exec_query_stats_indx') IS NOT NULL
  DROP TABLE #tmpdm_exec_query_stats_indx

SELECT *
INTO #tmpdm_exec_query_stats_indx 
FROM sys.dm_exec_query_stats
WHERE dm_exec_query_stats.total_worker_time > 0 /* Only plans with CPU time > 0ms */
AND dm_exec_query_stats.query_plan_hash <> 0x0000000000000000
AND NOT EXISTS(SELECT 1 
                 FROM sys.dm_exec_cached_plans
                 WHERE dm_exec_cached_plans.plan_handle = dm_exec_query_stats.plan_handle
                 AND dm_exec_cached_plans.cacheobjtype = 'Compiled Plan Stub') /*Ignoring AdHoc - Plan Stub*/
AND @skipcache = 0
OPTION (RECOMPILE);

CREATE CLUSTERED INDEX ixquery_hash ON #tmpdm_exec_query_stats_indx(query_hash, last_execution_time)

SELECT TOP (@TOP)
       CONVERT(INT, NULL) AS database_id,
       CONVERT(INT, NULL) AS object_id,
       CONVERT(sysname, NULL) AS object_name,
       query_hash,
       plan_count,
       plan_generation_num,
       ISNULL(t_dm_exec_query_stats.plan_handle, 0x) AS plan_handle,
       ISNULL(statement_start_offset, 0) AS statement_start_offset,
       ISNULL(statement_end_offset, 0) AS statement_end_offset,
       CONVERT(XML, NULL) AS statement_plan, 
       CONVERT(XML, NULL) AS statement_text,
       creation_time,
       last_execution_time,
       /*
         Query impact is a calculated metric which represents the overall impact of the query on the server. 
         This allows you to identify the queries which need most attention.
         It is calculated FROM a combination of metrics as follows: 
         QueryImpact = log((TotalCPUTime x 3) + TotalLogicalReads + TotalLogicalWrites)
       */
       CONVERT(NUMERIC(25, 2), LOG((total_worker_time * 3) + total_logical_reads + total_logical_writes)) AS query_impact,
       execution_count,
       CASE 
         WHEN @total_execution_count = 0 THEN 0
         ELSE CONVERT(NUMERIC(25, 2), (100. * execution_count) / @total_execution_count) 
       END AS execution_count_percent_over_total,
       execution_count / CASE WHEN DATEDIFF(MINUTE, creation_time, last_execution_time) = 0 THEN 1 ELSE DATEDIFF(MINUTE, creation_time, last_execution_time) END AS execution_count_per_minute,
       CONVERT(BIGINT, NULL) AS execution_count_current,
       CONVERT(BIGINT, NULL) AS execution_count_last_minute,

       CONVERT(NUMERIC(25, 4), (total_elapsed_time) /1000. /1000.) AS total_elapsed_time_sec,
       CASE 
         WHEN @total_elapsed_time = 0 THEN 0
         ELSE CONVERT(NUMERIC(25, 2), (100. * total_elapsed_time) / @total_elapsed_time) 
       END AS elapsed_time_sec_percent_over_total,
       CONVERT(NUMERIC(25, 4), (total_elapsed_time / execution_count) /1000. /1000.) AS avg_elapsed_time_sec,
       CONVERT(NUMERIC(25, 4), min_elapsed_time /1000. /1000.) AS min_elapsed_time_sec,
       CONVERT(NUMERIC(25, 4), max_elapsed_time /1000. /1000.) AS max_elapsed_time_sec,
       CONVERT(NUMERIC(25, 4), last_elapsed_time /1000. /1000.) AS last_elapsed_time_sec,

       CONVERT(NUMERIC(25, 4), (total_worker_time) /1000. /1000.) AS total_cpu_time_sec,
       CASE 
         WHEN @total_worker_time = 0 THEN 0
         ELSE CONVERT(NUMERIC(25, 2), (100. * total_worker_time) / @total_worker_time) 
       END AS cpu_time_sec_percent_over_total,
       CONVERT(NUMERIC(25, 4), (total_worker_time / execution_count) /1000. /1000.) AS avg_cpu_time_sec,
       CONVERT(NUMERIC(25, 4), min_worker_time /1000. /1000.) AS min_cpu_time_sec,
       CONVERT(NUMERIC(25, 4), max_worker_time /1000. /1000.) AS max_cpu_time_sec,
       CONVERT(NUMERIC(25, 4), last_worker_time /1000. /1000.) AS last_cpu_time_sec,

       total_logical_reads AS total_logical_page_reads,
       CASE 
         WHEN @total_logical_page_reads = 0 THEN 0
         ELSE CONVERT(NUMERIC(25, 2), (100. * total_logical_reads) / @total_logical_page_reads) 
       END AS logical_page_reads_percent_over_total,
       CONVERT(BIGINT, (total_logical_reads / execution_count)) AS avg_logical_page_reads,
       min_logical_reads AS min_logical_page_reads,
       max_logical_reads AS max_logical_page_reads,
       last_logical_reads AS last_logical_page_reads,

       CONVERT(NUMERIC(25, 4), total_logical_reads * 8 / 1024. / 1024.) AS total_logical_reads_gb,
       CASE 
         WHEN @total_logical_page_reads = 0 THEN 0
         ELSE CONVERT(NUMERIC(25, 2), (100. * total_logical_reads) / @total_logical_page_reads) 
       END AS logical_reads_gb_percent_over_total,
       CONVERT(NUMERIC(25, 4), CONVERT(BIGINT, (total_logical_reads / execution_count)) * 8 / 1024. / 1024.) AS avg_logical_reads_gb,
       CONVERT(NUMERIC(25, 4), min_logical_reads * 8 / 1024. / 1024.) AS min_logical_reads_gb,
       CONVERT(NUMERIC(25, 4), max_logical_reads * 8 / 1024. / 1024.) AS max_logical_reads_gb,
       CONVERT(NUMERIC(25, 4), last_logical_reads * 8 / 1024. / 1024.) AS last_logical_reads_gb,

       total_physical_reads AS total_physical_page_reads,
       CASE 
         WHEN @total_physical_page_reads = 0 THEN 0
         ELSE CONVERT(NUMERIC(25, 2), (100. * total_physical_reads) / @total_physical_page_reads) 
       END AS physical_page_reads_percent_over_total,
       CONVERT(BIGINT, (total_physical_reads / execution_count)) AS avg_physical_page_reads,
       min_physical_reads AS min_physical_page_reads,
       max_physical_reads AS max_physical_page_reads,
       last_physical_reads AS last_physical_page_reads,

       CONVERT(NUMERIC(25, 4), total_physical_reads * 8 / 1024. / 1024.) AS total_physical_reads_gb,
       CASE 
         WHEN @total_physical_page_reads = 0 THEN 0
         ELSE CONVERT(NUMERIC(25, 2), (100. * total_physical_reads) / @total_physical_page_reads) 
       END AS physical_reads_gb_percent_over_total,
       CONVERT(NUMERIC(25, 4), CONVERT(BIGINT, (total_physical_reads / execution_count)) * 8 / 1024. / 1024.) AS avg_physical_reads_gb,
       CONVERT(NUMERIC(25, 4), min_physical_reads * 8 / 1024. / 1024.) AS min_physical_reads_gb,
       CONVERT(NUMERIC(25, 4), max_physical_reads * 8 / 1024. / 1024.) AS max_physical_reads_gb,
       CONVERT(NUMERIC(25, 4), last_physical_reads * 8 / 1024. / 1024.) AS last_physical_reads_gb,

       total_logical_writes AS total_logical_page_writes,
       CASE 
         WHEN @total_logical_page_writes = 0 THEN 0
         ELSE CONVERT(NUMERIC(25, 2), (100. * total_logical_writes) / @total_logical_page_writes) 
       END AS logical_page_writes_percent_over_total,
       CONVERT(BIGINT, (total_logical_writes / execution_count)) AS avglogical_page_writes,
       min_logical_writes AS min_logical_page_writes,
       max_logical_writes AS max_logical_page_writes,
       last_logical_writes AS last_logical_page_writes,

       CONVERT(NUMERIC(25, 4), total_logical_writes * 8 / 1024. / 1024.) AS total_logical_writes_gb,
       CASE 
         WHEN @total_logical_page_writes = 0 THEN 0
         ELSE CONVERT(NUMERIC(25, 2), (100. * total_logical_writes) / @total_logical_page_writes)
       END AS logical_writes_gb_percent_over_total,
       CONVERT(NUMERIC(25, 4), CONVERT(BIGINT, (total_physical_reads / execution_count)) * 8 / 1024. / 1024.) AS avg_logical_writes_gb,
       CONVERT(NUMERIC(25, 4), min_logical_writes * 8 / 1024. / 1024.) AS min_logical_writes_gb,
       CONVERT(NUMERIC(25, 4), max_logical_writes * 8 / 1024. / 1024.) AS max_logical_writes_gb,
       CONVERT(NUMERIC(25, 4), last_logical_writes * 8 / 1024. / 1024.) AS last_logical_writes_gb,

       total_rows AS total_returned_rows,
       CONVERT(BIGINT, (total_rows / execution_count)) AS avg_returned_rows,
       min_rows AS min_returned_rows,
       max_rows AS max_returned_rows,
       last_rows AS last_returned_rows,
       CONVERT(NUMERIC(25, 4), dm_exec_cached_plans.size_in_bytes / 1024. / 1024.) AS cached_plan_size_mb
INTO #tmpdm_exec_query_stats
FROM (SELECT query_hash,
             COUNT(DISTINCT query_plan_hash)          AS plan_count,
             MAX(t_last_value.plan_handle)            AS plan_handle,
             MAX(t_last_value.statement_start_offset) AS statement_start_offset,
             MAX(t_last_value.statement_end_offset)   AS statement_end_offset,
             MAX(t_last_value.plan_generation_num)    AS plan_generation_num,
             MAX(t_last_value.creation_time)          AS creation_time,
             MAX(t_last_value.last_execution_time)    AS last_execution_time,
             SUM(execution_count)                     AS execution_count,
             SUM(total_worker_time)                   AS total_worker_time,
             MAX(t_last_value.last_worker_time)       AS last_worker_time,
             MIN(min_worker_time)                     AS min_worker_time,
             MAX(max_worker_time)                     AS max_worker_time,
             SUM(total_physical_reads)                AS total_physical_reads,
             MAX(t_last_value.last_physical_reads)    AS last_physical_reads,
             MIN(min_physical_reads)                  AS min_physical_reads,
             MAX(max_physical_reads)                  AS max_physical_reads,
             SUM(total_logical_writes)                AS total_logical_writes,
             MAX(t_last_value.last_logical_writes)    AS last_logical_writes,
             MIN(min_logical_writes)                  AS min_logical_writes,
             MAX(max_logical_writes)                  AS max_logical_writes,
             SUM(total_logical_reads)                 AS total_logical_reads,
             MAX(t_last_value.last_logical_reads)     AS last_logical_reads,
             MIN(min_logical_reads)                   AS min_logical_reads,
             MAX(max_logical_reads)                   AS max_logical_reads,
             SUM(total_elapsed_time)                  AS total_elapsed_time,
             MAX(t_last_value.last_elapsed_time)      AS last_elapsed_time,
             MIN(min_elapsed_time)                    AS min_elapsed_time,
             MAX(max_elapsed_time)                    AS max_elapsed_time,
             SUM(total_rows)                          AS total_rows,
             MAX(t_last_value.last_rows)              AS last_rows,
             MIN(min_rows)                            AS min_rows,
             MAX(max_rows)                            AS max_rows
      FROM #tmpdm_exec_query_stats_indx
      CROSS APPLY (SELECT TOP 1 plan_handle,
                                statement_start_offset, 
                                statement_end_offset,
                                plan_generation_num,
                                creation_time,
                                last_execution_time, 
                                last_worker_time, 
                                last_physical_reads, 
                                last_logical_writes, 
                                last_logical_reads, 
                                last_elapsed_time, 
                                last_rows
                   FROM #tmpdm_exec_query_stats_indx AS b
                   WHERE b.query_hash = #tmpdm_exec_query_stats_indx.query_hash
                   ORDER BY last_execution_time DESC) AS t_last_value
      GROUP BY query_hash) AS t_dm_exec_query_stats
INNER JOIN sys.dm_exec_cached_plans
ON dm_exec_cached_plans.plan_handle = t_dm_exec_query_stats.plan_handle
ORDER BY query_impact DESC
OPTION (RECOMPILE);

ALTER TABLE #tmpdm_exec_query_stats ADD CONSTRAINT pk_sp_getindexinfo_tmpdm_exec_query_stats
PRIMARY KEY (plan_handle, statement_start_offset, statement_end_offset)

DECLARE @number_plans BIGINT,
        @query_hash   VARBINARY(64),
        @plan_handle  VARBINARY(64),
        @statement_start_offset BIGINT, 
        @statement_end_offset BIGINT,
        @i            BIGINT

SELECT @number_plans = COUNT(*) 
FROM #tmpdm_exec_query_stats

SELECT @statusMsg = '[' + CONVERT(NVARCHAR(200), GETDATE(), 120) + '] - ' + 'Starting to capture XML query plan and statement text for cached plans. Found ' + CONVERT(VARCHAR(200), @number_plans) + ' plans on sys.dm_exec_query_stats.'
RAISERROR (@statusMsg, 0, 0) WITH NOWAIT

SET @i = 1
DECLARE c_plans CURSOR FORWARD_ONLY READ_ONLY FOR
    SELECT query_hash, plan_handle, statement_start_offset, statement_end_offset 
    FROM #tmpdm_exec_query_stats
OPEN c_plans

FETCH NEXT FROM c_plans
INTO @query_hash, @plan_handle, @statement_start_offset, @statement_end_offset
WHILE @@FETCH_STATUS = 0
BEGIN
  BEGIN TRY
    SET @statusMsg = '[' + CONVERT(VARCHAR(200), GETDATE(), 120) + '] - ' + 'Progress ' + '(' + CONVERT(VARCHAR(200), CONVERT(NUMERIC(25, 2), (CONVERT(NUMERIC(25, 2), @i) / CONVERT(NUMERIC(25, 2), @number_plans)) * 100)) + '%%) - ' 
                   + CONVERT(VARCHAR(200), @i) + ' of ' + CONVERT(VARCHAR(200), @number_plans)
    IF @i % 100 = 0
      RAISERROR (@statusMsg, 0, 1) WITH NOWAIT

    ;WITH XMLNAMESPACES('http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p)
    UPDATE #tmpdm_exec_query_stats SET database_id = detqp.dbid,
                                       object_id = detqp.objectid,
                                       object_name = OBJECT_NAME(detqp.objectid, detqp.dbid),
                                       statement_plan = CASE 
                                                          WHEN detqp.encrypted = 1 THEN '<?query ---- Plan is encrypted. ----?>'
                                                          /* If conversion of query_plan text to XML is not possible, return plan has a text.
                                                             One of most common reasons it may not able to convert the text to XML is due to the 
                                                             "XML datatype instance has too many levels of nested nodes. Maximum allowed depth is 128 levels." limitation.*/
                                                          WHEN detqp.query_plan IS NOT NULL AND t0.query_plan IS NULL THEN '<?query ---- ' + NCHAR(13) + NCHAR(10) + detqp.query_plan + NCHAR(13) + NCHAR(10) + ' ----?>'
                                                          ELSE t0.query_plan
                                                        END,
                                       statement_text = CASE detqp.encrypted WHEN 1 THEN '<?query ---- Stmt is encrypted. ----?>' ELSE t2.cStatement END
    FROM #tmpdm_exec_query_stats AS qs
    CROSS APPLY sys.dm_exec_text_query_plan(qs.plan_handle,
                                            qs.statement_start_offset,
                                            qs.statement_end_offset) AS detqp
    OUTER APPLY (SELECT TRY_CONVERT(XML, STUFF(detqp.query_plan,
                                               CHARINDEX(N'<BatchSequence>', detqp.query_plan),
                                               0,
                                               (
                                                   SELECT ISNULL(stat.c_data,'<?dm_exec_query_stats ---- QueryStats not found. ----?>') + 
                                                          ISNULL(attrs.c_data, '<?dm_exec_plan_attributes ---- PlanAttributes not found. ----?>')
                                                   FROM (
                                                           SELECT t_last_value.*
                                                           FROM #tmpdm_exec_query_stats_indx AS a
                                                           CROSS APPLY (SELECT TOP 1 b.*
                                                                        FROM #tmpdm_exec_query_stats_indx AS b
                                                                        WHERE b.query_hash = a.query_hash
                                                                        ORDER BY b.last_execution_time DESC) AS t_last_value
                                                           WHERE a.plan_handle = @plan_handle
                                                           AND a.statement_start_offset = @statement_start_offset
                                                           AND a.statement_end_offset = @statement_end_offset
                                                           FOR XML RAW('Stats'), ROOT('dm_exec_query_stats'), BINARY BASE64
                                                       ) AS stat(c_data)
                                                   OUTER APPLY (
                                                           SELECT pvt.*
                                                           FROM (
                                                               SELECT epa.attribute, epa.value
                                                               FROM sys.dm_exec_plan_attributes(@plan_handle) AS epa) AS ecpa   
                                                           PIVOT (MAX(ecpa.value) FOR ecpa.attribute IN ("set_options","objectid","dbid","dbid_execute","user_id","language_id","date_format","date_first","compat_level","status","required_cursor_options","acceptable_cursor_options","merge_action_type","is_replication_specific","optional_spid","optional_clr_trigger_dbid","optional_clr_trigger_objid","parent_plan_handle","inuse_exec_context","free_exec_context","hits_exec_context","misses_exec_context","removed_exec_context","inuse_cursors","free_cursors","hits_cursors","misses_cursors","removed_cursors","sql_handle")) AS pvt
                                                           FOR XML RAW('Attr'), ROOT('dm_exec_plan_attributes'), BINARY BASE64
                                                       ) AS attrs(c_data)
                                                   )
                                               ))) AS t0 (query_plan)
    OUTER APPLY t0.query_plan.nodes('//p:Batch') AS Batch(x)
    OUTER APPLY (SELECT COALESCE(Batch.x.value('(//p:StmtSimple/@StatementText)[1]', 'VarChar(MAX)'),
                                 Batch.x.value('(//p:StmtCond/@StatementText)[1]', 'VarChar(MAX)'),
                                 Batch.x.value('(//p:StmtCursor/@StatementText)[1]', 'VarChar(MAX)'),
                                 Batch.x.value('(//p:StmtReceive/@StatementText)[1]', 'VarChar(MAX)'),
                                 Batch.x.value('(//p:StmtUseDb/@StatementText)[1]', 'VarChar(MAX)')) AS query) AS t1
    OUTER APPLY (SELECT CONVERT(XML, ISNULL(CONVERT(XML, '<?query --' +
                                                            REPLACE
					                                                       (
						                                                       REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
						                                                       REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
						                                                       REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
							                                                       CONVERT
							                                                       (
								                                                       VARCHAR(MAX),
								                                                       N'--' + NCHAR(13) + NCHAR(10) + t1.query + NCHAR(13) + NCHAR(10) + NCHAR(13) + NCHAR(10) + '/* Note: Query text was retrieved from showplan XML, and may be truncated. */' + NCHAR(13) + NCHAR(10) + N'--' COLLATE Latin1_General_Bin2
							                                                       ),
							                                                       NCHAR(31),N'?'),NCHAR(30),N'?'),NCHAR(29),N'?'),NCHAR(28),N'?'),NCHAR(27),N'?'),NCHAR(26),N'?'),NCHAR(25),N'?'),NCHAR(24),N'?'),NCHAR(23),N'?'),NCHAR(22),N'?'),
							                                                       NCHAR(21),N'?'),NCHAR(20),N'?'),NCHAR(19),N'?'),NCHAR(18),N'?'),NCHAR(17),N'?'),NCHAR(16),N'?'),NCHAR(15),N'?'),NCHAR(14),N'?'),NCHAR(12),N'?'),
							                                                       NCHAR(11),N'?'),NCHAR(8),N'?'),NCHAR(7),N'?'),NCHAR(6),N'?'),NCHAR(5),N'?'),NCHAR(4),N'?'),NCHAR(3),N'?'),NCHAR(2),N'?'),NCHAR(1),N'?'),
						                                                       NCHAR(0),
						                                                       N'')
                                                             + '--?>'),
                                                  '<?query --' + NCHAR(13) + NCHAR(10) +
                                                  'Statement not found.' + NCHAR(13) + NCHAR(10) +
                                                  '--?>'))) AS t2 (cStatement)
    WHERE qs.plan_handle = @plan_handle
    AND qs.statement_start_offset = @statement_start_offset
    AND qs.statement_end_offset = @statement_end_offset

    /* If wasn't able to extract text from the query plan, try to get it from the very slow sys.dm_exec_sql_text DMF */
    IF EXISTS(SELECT 1 FROM #tmpdm_exec_query_stats AS qs
               WHERE qs.plan_handle = @plan_handle
               AND qs.statement_start_offset = @statement_start_offset
               AND qs.statement_end_offset = @statement_end_offset
               AND CONVERT(VARCHAR(MAX), qs.statement_text) LIKE '%Statement not found.%')
    BEGIN
      UPDATE #tmpdm_exec_query_stats SET database_id = st.dbid,
                                         object_id = st.objectid,
                                         object_name = OBJECT_NAME(st.objectid, st.dbid),
                                         statement_text = CASE st.encrypted WHEN 1 THEN '<?query ---- Stmt is encrypted. ----?>' ELSE t2.cStatement END
      FROM #tmpdm_exec_query_stats AS qs
      OUTER APPLY sys.dm_exec_sql_text(qs.plan_handle) st
      CROSS APPLY (SELECT ISNULL(
                              NULLIF(
                                  SUBSTRING(
                                    st.text, 
                                    (qs.statement_start_offset / 2) + 1,
                                    CASE WHEN qs.statement_end_offset < qs.statement_start_offset 
                                     THEN 0
                                    ELSE (qs.statement_end_offset - qs.statement_start_offset) / 2 END + 2
                                  ), ''
                              ), st.text
                          )) AS t1(Query)
      CROSS APPLY (SELECT TRY_CONVERT(XML, ISNULL(TRY_CONVERT(XML, 
                                                              '<?query --' +
                                                              REPLACE
					                                                         (
						                                                         REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
						                                                         REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
						                                                         REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
							                                                         CONVERT
							                                                         (
								                                                         VARCHAR(MAX),
								                                                         N'--' + NCHAR(13) + NCHAR(10) + t1.query + NCHAR(13) + NCHAR(10) + N'--' COLLATE Latin1_General_Bin2
							                                                         ),
							                                                         NCHAR(31),N'?'),NCHAR(30),N'?'),NCHAR(29),N'?'),NCHAR(28),N'?'),NCHAR(27),N'?'),NCHAR(26),N'?'),NCHAR(25),N'?'),NCHAR(24),N'?'),NCHAR(23),N'?'),NCHAR(22),N'?'),
							                                                         NCHAR(21),N'?'),NCHAR(20),N'?'),NCHAR(19),N'?'),NCHAR(18),N'?'),NCHAR(17),N'?'),NCHAR(16),N'?'),NCHAR(15),N'?'),NCHAR(14),N'?'),NCHAR(12),N'?'),
							                                                         NCHAR(11),N'?'),NCHAR(8),N'?'),NCHAR(7),N'?'),NCHAR(6),N'?'),NCHAR(5),N'?'),NCHAR(4),N'?'),NCHAR(3),N'?'),NCHAR(2),N'?'),NCHAR(1),N'?'),
						                                                         NCHAR(0),
						                                                         N'')
                                                               + '--?>'),
                                                    '<?query --' + NCHAR(13) + NCHAR(10) +
                                                    'Could not render the query due to XML data type limitations.' + NCHAR(13) + NCHAR(10) +
                                                    '--?>'))) AS t2 (cStatement)
      WHERE qs.plan_handle = @plan_handle
      AND qs.statement_start_offset = @statement_start_offset
      AND qs.statement_end_offset = @statement_end_offset
    END
		END TRY
		BEGIN CATCH
			 --SELECT @statusMsg = '[' + CONVERT(NVARCHAR(200), GETDATE(), 120) + '] - ' + 'Error trying to work on plan [' + CONVERT(NVARCHAR(800), @plan_handle, 1) + ']. Skipping this plan.'
    --RAISERROR (@statusMsg, 0, 0) WITH NOWAIT
    --SELECT @statusMsg = '[' + CONVERT(NVARCHAR(200), GETDATE(), 120) + '] - ' + ERROR_MESSAGE() 
    --RAISERROR (@statusMsg, 0, 0) WITH NOWAIT
		END CATCH

  SET @i = @i + 1
  FETCH NEXT FROM c_plans
  INTO @query_hash, @plan_handle, @statement_start_offset, @statement_end_offset
END
CLOSE c_plans
DEALLOCATE c_plans

SELECT @statusMsg = '[' + CONVERT(NVARCHAR(200), GETDATE(), 120) + '] - ' + 'Finished to capture XML query plan and statement text for cached plans.'
RAISERROR (@statusMsg, 0, 0) WITH NOWAIT

--SELECT @statusMsg = '[' + CONVERT(NVARCHAR(200), GETDATE(), 120) + '] - ' + 'Starting to remove plans bigger than 2MB.'
--RAISERROR (@statusMsg, 0, 0) WITH NOWAIT

--DECLARE @removed_plans INT = 0
--DELETE FROM #tmpdm_exec_query_stats 
--WHERE DATALENGTH(statement_plan) / 1024. > 2048 /*Ignoring big plans to avoid delay and issues when exporting it to Excel*/

--SET @removed_plans = @@ROWCOUNT

--SELECT @statusMsg = '[' + CONVERT(NVARCHAR(200), GETDATE(), 120) + '] - ' + 'Finished to remove plans bigger than 2MB, removed ' + CONVERT(VARCHAR, ISNULL(@removed_plans, 0)) + ' plans.'
--RAISERROR (@statusMsg, 0, 0) WITH NOWAIT

SELECT @statusMsg = '[' + CONVERT(NVARCHAR(200), GETDATE(), 120) + '] - ' + 'Starting to collect data about last minute execution count.'
RAISERROR (@statusMsg, 0, 0) WITH NOWAIT

/* Update execution_count_current with current number of executions */
UPDATE #tmpdm_exec_query_stats SET execution_count_current = dm_exec_query_stats.execution_count
FROM #tmpdm_exec_query_stats AS qs
INNER JOIN sys.dm_exec_query_stats
ON qs.plan_handle = dm_exec_query_stats.plan_handle
AND qs.statement_start_offset = dm_exec_query_stats.statement_start_offset
AND qs.statement_end_offset = dm_exec_query_stats.statement_end_offset

/* Wait for 1 minute */
IF (@@SERVERNAME NOT LIKE '%amorim%') AND (@@SERVERNAME NOT LIKE '%fabiano%')
BEGIN
  WAITFOR DELAY '00:01:00.000'
END

/* Update execution_count_last_minute with number of executions on last minute */
UPDATE #tmpdm_exec_query_stats SET execution_count_last_minute = dm_exec_query_stats.execution_count - qs.execution_count_current
FROM #tmpdm_exec_query_stats AS qs
INNER JOIN sys.dm_exec_query_stats
ON qs.plan_handle = dm_exec_query_stats.plan_handle
AND qs.statement_start_offset = dm_exec_query_stats.statement_start_offset
AND qs.statement_end_offset = dm_exec_query_stats.statement_end_offset

SELECT @statusMsg = '[' + CONVERT(NVARCHAR(200), GETDATE(), 120) + '] - ' + 'Finished to update data about last minute execution count.'
RAISERROR (@statusMsg, 0, 0) WITH NOWAIT

SELECT @statusMsg = '[' + CONVERT(NVARCHAR(200), GETDATE(), 120) + '] - ' + 'Starting to create XML indexes on #tmpdm_exec_query_stats.'
RAISERROR (@statusMsg, 0, 0) WITH NOWAIT

CREATE PRIMARY XML INDEX ix1 ON #tmpdm_exec_query_stats(statement_plan)
CREATE XML INDEX ix2 ON #tmpdm_exec_query_stats(statement_plan)
USING XML INDEX ix1 FOR PROPERTY

SELECT @statusMsg = '[' + CONVERT(NVARCHAR(200), GETDATE(), 120) + '] - ' + 'Finished to create XML indexes on #tmpdm_exec_query_stats.'
RAISERROR (@statusMsg, 0, 0) WITH NOWAIT

SELECT @statusMsg = '[' + CONVERT(NVARCHAR(200), GETDATE(), 120) + '] - ' + 'Starting to run final query and parse query plan XML and populate tmpIndexCheckCachePlanData'
RAISERROR (@statusMsg, 0, 0) WITH NOWAIT

IF OBJECT_ID('tempdb.dbo.tmpIndexCheckCachePlanData') IS NOT NULL
  DROP TABLE tempdb.dbo.tmpIndexCheckCachePlanData

CREATE TABLE tempdb.dbo.tmpIndexCheckCachePlanData
(
  [database_name] [sys].[sysname] NULL,
  [object_name] [sys].[sysname] NULL,
  [query_hash] [varchar] (800) NULL,
  [plan_handle] [varchar] (800) NULL,
  [query_impact] [numeric] (25, 2) NULL,
  [number_of_referenced_indexes] [bigint] NOT NULL,
  [index_list] [xml] NULL,
  [number_of_referenced_stats] [bigint] NOT NULL,
  [stats_list] [xml] NULL,
  [sum_modification_count_for_all_used_stats] [float] NULL,
  [statement_text] [xml] NULL,
  [statement_plan] [xml] NULL,
  [execution_count] [bigint] NULL,
  [execution_count_percent_over_total] [numeric] (25, 2) NULL,
  [execution_count_per_minute] [bigint] NULL,
  [execution_count_current] [bigint] NULL,
  [execution_count_last_minute] [bigint] NULL,
  [compilation_time_from_dm_exec_query_stats] [int] NULL,
  [exec_plan_creation_start_datetime] [varchar] (30) NULL,
  [last_execution_datetime] [datetime] NULL,
  [cached_plan_size_mb] [numeric] (25, 4) NULL,
  [statement_cached_plan_size_mb] [numeric] (25, 4) NULL,
  [cached_plan_size_status] [varchar] (50) NOT NULL,
  [statement_type] [varchar] (500) NULL,
  [ce_model_version] [int] NULL,
  [statement_optm_early_abort_reason] [sys].[sysname] NULL,
  [query_plan_cost] [float] NULL,
  [cost_threshold_for_parallelism] [int] NULL,
  [is_parallel] [bit] NOT NULL,
  [has_serial_ordered_backward_scan] [bit] NULL,
  [compile_time_sec] [numeric] (25, 4) NULL,
  [compile_cpu_sec] [numeric] (25, 4) NULL,
  [compile_memory_mb] [numeric] (25, 4) NULL,
  [serial_desired_memory_mb] [numeric] (25, 4) NULL,
  [serial_required_memory_mb] [numeric] (25, 4) NULL,
  [missing_index_count] [int] NULL,
  [warning_count] [int] NULL,
  [has_implicit_conversion_warning] [bit] NOT NULL,
  [has_no_join_predicate_warning] [bit] NULL,
  [operator_max_estimated_rows] [float] NULL,
  [has_nested_loop_join] [bit] NULL,
  [has_merge_join] [bit] NULL,
  [has_hash_join] [bit] NULL,
  [has_many_to_many_merge_join] [bit] NULL,
  [has_join_residual_predicate] [bit] NULL,
  [has_index_seek_residual_predicate] [bit] NULL,
  [has_key_or_rid_lookup] [bit] NULL,
  [has_spilling_operators] [bit] NULL,
  [has_remote_operators] [bit] NULL,
  [has_spool_operators] [bit] NULL,
  [has_index_spool_operators] [bit] NULL,
  [has_table_scan_on_heap] [bit] NULL,
  [has_table_valued_functions] [bit] NULL,
  [has_user_defined_function] [bit] NULL,
  [has_partitioned_tables] [bit] NULL,
  [has_min_max_agg] [bit] NOT NULL,
  [is_prefetch_enabled] [bit] NULL,
  [has_parameter_sniffing_problem] [int] NULL,
  [is_parameterized] [bit] NULL,
  [is_using_table_variable] [bit] NULL,
  [total_elapsed_time_sec] [numeric] (25, 4) NULL,
  [elapsed_time_sec_percent_over_total] [numeric] (25, 2) NULL,
  [avg_elapsed_time_sec] [numeric] (25, 4) NULL,
  [min_elapsed_time_sec] [numeric] (25, 4) NULL,
  [max_elapsed_time_sec] [numeric] (25, 4) NULL,
  [last_elapsed_time_sec] [numeric] (25, 4) NULL,
  [total_cpu_time_sec] [numeric] (25, 4) NULL,
  [cpu_time_sec_percent_over_total] [numeric] (25, 2) NULL,
  [avg_cpu_time_sec] [numeric] (25, 4) NULL,
  [min_cpu_time_sec] [numeric] (25, 4) NULL,
  [max_cpu_time_sec] [numeric] (25, 4) NULL,
  [last_cpu_time_sec] [numeric] (25, 4) NULL,
  [total_logical_page_reads] [bigint] NULL,
  [logical_page_reads_percent_over_total] [numeric] (25, 2) NULL,
  [avg_logical_page_reads] [bigint] NULL,
  [min_logical_page_reads] [bigint] NULL,
  [max_logical_page_reads] [bigint] NULL,
  [last_logical_page_reads] [bigint] NULL,
  [total_logical_reads_gb] [numeric] (25, 4) NULL,
  [logical_reads_gb_percent_over_total] [numeric] (25, 2) NULL,
  [avg_logical_reads_gb] [numeric] (25, 4) NULL,
  [min_logical_reads_gb] [numeric] (25, 4) NULL,
  [max_logical_reads_gb] [numeric] (25, 4) NULL,
  [last_logical_reads_gb] [numeric] (25, 4) NULL,
  [total_physical_page_reads] [bigint] NULL,
  [physical_page_reads_percent_over_total] [numeric] (25, 2) NULL,
  [avg_physical_page_reads] [bigint] NULL,
  [min_physical_page_reads] [bigint] NULL,
  [max_physical_page_reads] [bigint] NULL,
  [last_physical_page_reads] [bigint] NULL,
  [total_physical_reads_gb] [numeric] (25, 4) NULL,
  [physical_reads_gb_percent_over_total] [numeric] (25, 2) NULL,
  [avg_physical_reads_gb] [numeric] (25, 4) NULL,
  [min_physical_reads_gb] [numeric] (25, 4) NULL,
  [max_physical_reads_gb] [numeric] (25, 4) NULL,
  [last_physical_reads_gb] [numeric] (25, 4) NULL,
  [total_logical_page_writes] [bigint] NULL,
  [logical_page_writes_percent_over_total] [numeric] (25, 2) NULL,
  [avglogical_page_writes] [bigint] NULL,
  [min_logical_page_writes] [bigint] NULL,
  [max_logical_page_writes] [bigint] NULL,
  [last_logical_page_writes] [bigint] NULL,
  [total_logical_writes_gb] [numeric] (25, 4) NULL,
  [logical_writes_gb_percent_over_total] [numeric] (25, 2) NULL,
  [avg_logical_writes_gb] [numeric] (25, 4) NULL,
  [min_logical_writes_gb] [numeric] (25, 4) NULL,
  [max_logical_writes_gb] [numeric] (25, 4) NULL,
  [last_logical_writes_gb] [numeric] (25, 4) NULL,
  [total_returned_rows] [bigint] NULL,
  [avg_returned_rows] [bigint] NULL,
  [min_returned_rows] [bigint] NULL,
  [max_returned_rows] [bigint] NULL,
  [last_returned_rows] [bigint] NULL
)

DECLARE @ctp INT;
SELECT  @ctp = CAST(value AS INT)
FROM    sys.configurations
WHERE   name = 'cost threshold for parallelism'
OPTION (RECOMPILE);

/* Setting "parameter sniffing variance percent" to 30% */
DECLARE @parameter_sniffing_warning_pct TINYINT = 30;
/* Setting min number of rows to be considered on PSP to 100*/
DECLARE @parameter_sniffing_rows_threshold TINYINT = 100;

SET @i = 1
DECLARE c_plans CURSOR FORWARD_ONLY READ_ONLY FOR
    SELECT query_hash, plan_handle, statement_start_offset, statement_end_offset 
    FROM #tmpdm_exec_query_stats
OPEN c_plans

FETCH NEXT FROM c_plans
INTO @query_hash, @plan_handle, @statement_start_offset, @statement_end_offset
WHILE @@FETCH_STATUS = 0
BEGIN
  BEGIN TRY
    SET @statusMsg = '[' + CONVERT(VARCHAR(200), GETDATE(), 120) + '] - ' + 'Progress ' + '(' + CONVERT(VARCHAR(200), CONVERT(NUMERIC(25, 2), (CONVERT(NUMERIC(25, 2), @i) / CONVERT(NUMERIC(25, 2), @number_plans)) * 100)) + '%%) - ' 
                   + CONVERT(VARCHAR(200), @i) + ' of ' + CONVERT(VARCHAR(200), @number_plans)
    IF @i % 100 = 0
      RAISERROR (@statusMsg, 0, 1) WITH NOWAIT

    ;WITH XMLNAMESPACES('http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p)
    INSERT INTO tempdb.dbo.tmpIndexCheckCachePlanData WITH(TABLOCK)
    SELECT  CASE database_id 
              WHEN 32767 THEN 'ResourceDB' 
              ELSE DB_NAME(database_id)
            END AS database_name,
            object_name,
            CONVERT(VARCHAR(800), query_hash, 1) AS query_hash,
            CONVERT(VARCHAR(800), plan_handle, 1) AS plan_handle,
            query_impact,
            ISNULL(LEN(t_index_list.index_list) - LEN(REPLACE(t_index_list.index_list, ',', '')) + 1, 0) AS number_of_referenced_indexes,
            CONVERT(XML, ISNULL(t_index_list.index_list,'')) AS index_list,
            ISNULL(LEN(t_stats_list.stats_list) - LEN(REPLACE(t_stats_list.stats_list, ',', '')) + 1, 0) AS number_of_referenced_stats,
            CONVERT(XML, ISNULL(t_stats_list.stats_list,'')) AS stats_list,
            Batch.x.value('sum(//p:OptimizerStatsUsage/p:StatisticsInfo/@ModificationCount)', 'float') AS sum_modification_count_for_all_used_stats,
            statement_text,
            statement_plan,
            execution_count,
            execution_count_percent_over_total,
            execution_count_per_minute,
            execution_count_current,
            execution_count_last_minute,
            /* 
               If there is only one execution, then, the compilation time can be calculated by
               checking the diff from the creation_time and last_execution_time.
               This is possible because creation_time is the time which the plan started creation
               and last_execution_time is the time which the plan started execution.
               So, for instance, considering the following:
               creation_time = "2022-11-09 07:56:19.123" 
               last_execution_time = "2022-11-09 07:56:26.937"
               This means, the plan started to be created at "2022-11-09 07:56:19.123" 
               and started execution at "2022-11-09 07:56:26.937", in other words, 
               it took 7813ms (DATEDIFF(ms, "2022-11-09 07:56:19.123" , "2022-11-09 07:56:26.937")) 
               to create the plan.
            */
            CASE 
             WHEN execution_count = 1
             THEN DATEDIFF(ms, creation_time, last_execution_time)
             ELSE NULL
            END AS compilation_time_from_dm_exec_query_stats,
            CONVERT(VARCHAR, creation_time, 21) AS exec_plan_creation_start_datetime,
            last_execution_time AS last_execution_datetime,
            cached_plan_size_mb,
            CONVERT(NUMERIC(25, 4), x.value('sum(..//p:QueryPlan/@CachedPlanSize)', 'float') / 1024.) AS statement_cached_plan_size_mb,
            CASE 
              WHEN cached_plan_size_mb >= 20 THEN 'Forget about it, don''t even try to see (over 20MB)'
              WHEN cached_plan_size_mb >= 15 THEN 'Planetarium plan (over 15MB)'
              WHEN cached_plan_size_mb >= 10 THEN 'Colossal plan (over 10MB)'
              WHEN cached_plan_size_mb >= 5 THEN 'Huge plan (over 5MB)'
              WHEN cached_plan_size_mb >= 2 THEN 'Big plan (over 2MB)'
              ELSE 'Normal plan (less than 2MB)'
            END AS cached_plan_size_status,
            COALESCE(Batch.x.value('(//p:StmtSimple/@StatementType)[1]', 'VarChar(500)'),
                     Batch.x.value('(//p:StmtCond/@StatementType)[1]', 'VarChar(500)'),
                     Batch.x.value('(//p:StmtCursor/@StatementType)[1]', 'VarChar(500)'),
                     Batch.x.value('(//p:StmtReceive/@StatementType)[1]', 'VarChar(500)'),
                     Batch.x.value('(//p:StmtUseDb/@StatementType)[1]', 'VarChar(500)')) AS statement_type,
            COALESCE(Batch.x.value('(//p:StmtSimple/@CardinalityEstimationModelVersion)[1]', 'int'),
                     Batch.x.value('(//p:StmtCond/@CardinalityEstimationModelVersion)[1]', 'int'),
                     Batch.x.value('(//p:StmtCursor/@CardinalityEstimationModelVersion)[1]', 'int'),
                     Batch.x.value('(//p:StmtReceive/@CardinalityEstimationModelVersion)[1]', 'int'),
                     Batch.x.value('(//p:StmtUseDb/@CardinalityEstimationModelVersion)[1]', 'int')) AS ce_model_version,
            COALESCE(Batch.x.value('(//p:StmtSimple/@StatementOptmEarlyAbortReason)[1]', 'sysname'),
                     Batch.x.value('(//p:StmtCond/@StatementOptmEarlyAbortReason)[1]', 'sysname'),
                     Batch.x.value('(//p:StmtCursor/@StatementOptmEarlyAbortReason)[1]', 'sysname'),
                     Batch.x.value('(//p:StmtReceive/@StatementOptmEarlyAbortReason)[1]', 'sysname'),
                     Batch.x.value('(//p:StmtUseDb/@StatementOptmEarlyAbortReason)[1]', 'sysname')) AS statement_optm_early_abort_reason,
            COALESCE(Batch.x.value('(//p:StmtSimple/@StatementSubTreeCost)[1]', 'float'),
                     Batch.x.value('(//p:StmtCond/@StatementSubTreeCost)[1]', 'float'),
                     Batch.x.value('(//p:StmtCursor/@StatementSubTreeCost)[1]', 'float'),
                     Batch.x.value('(//p:StmtReceive/@StatementSubTreeCost)[1]', 'float'),
                     Batch.x.value('(//p:StmtUseDb/@StatementSubTreeCost)[1]', 'float')) AS query_plan_cost,
            @ctp AS cost_threshold_for_parallelism,
            CASE WHEN Batch.x.value('max(//p:RelOp/@Parallel)', 'float') > 0 THEN 1 ELSE 0 END AS is_parallel,
            Batch.x.exist('(//p:IndexScan[@ScanDirection="BACKWARD" and @Ordered="1"])') AS has_serial_ordered_backward_scan,
            CONVERT(NUMERIC(25, 4), x.value('sum(..//p:QueryPlan/@CompileTime)', 'float') /1000.) AS compile_time_sec,
            CONVERT(NUMERIC(25, 4), x.value('sum(..//p:QueryPlan/@CompileCPU)', 'float') /1000.) AS compile_cpu_sec,
            CONVERT(NUMERIC(25, 4), x.value('sum(..//p:QueryPlan/@CompileMemory)', 'float') / 1024.) AS compile_memory_mb,
            CONVERT(NUMERIC(25, 4), Batch.x.value('sum(//p:MemoryGrantInfo/@SerialDesiredMemory)', 'float') / 1024.) AS serial_desired_memory_mb,
            CONVERT(NUMERIC(25, 4), Batch.x.value('sum(//p:MemoryGrantInfo/@SerialRequiredMemory)', 'float') / 1024.) AS serial_required_memory_mb,
            Batch.x.value('count(//p:MissingIndexGroup)', 'int') AS missing_index_count,
            Batch.x.value('count(//p:QueryPlan/p:Warnings/*)', 'int') AS warning_count,
            CASE WHEN Batch.x.exist('(//p:QueryPlan/p:Warnings/p:PlanAffectingConvert/@Expression[contains(., "CONVERT_IMPLICIT")])') = 1 THEN 1 ELSE 0 END AS has_implicit_conversion_warning,
            Batch.x.exist('//p:RelOp/p:Warnings[(@NoJoinPredicate[.="1"])]') AS has_no_join_predicate_warning,
            Batch.x.value('max(//p:RelOp/@EstimateRows)', 'float') AS operator_max_estimated_rows,
            Batch.x.exist('(//p:RelOp[contains(@PhysicalOp, "Nested Loops")])') AS has_nested_loop_join,
            Batch.x.exist('(//p:RelOp[contains(@PhysicalOp, "Merge Join")])') AS has_merge_join,
            Batch.x.exist('(//p:RelOp[contains(@PhysicalOp, "Hash Match")])') AS has_hash_join,
            Batch.x.exist('(//p:Merge/@ManyToMany[.="1"])') AS has_many_to_many_merge_join,
            Batch.x.exist('(//p:RelOp/p:Hash/p:ProbeResidual or //p:RelOp/p:Merge/p:Residual)') AS has_join_residual_predicate,
            Batch.x.exist('(//p:IndexScan/p:Predicate)') AS has_index_seek_residual_predicate,
            Batch.x.exist('(//p:RelOp[contains(@PhysicalOp, " Lookup")])') AS has_key_or_rid_lookup,
            Batch.x.exist('(//p:RelOp[contains(@PhysicalOp, "Sort") or contains(@PhysicalOp, "Hash Match")])') AS has_spilling_operators,
            Batch.x.exist('(//p:RelOp[contains(@PhysicalOp, "Remote")])') AS has_remote_operators,
            Batch.x.exist('(//p:RelOp[contains(@PhysicalOp, "Spool")])') AS has_spool_operators,
            Batch.x.exist('(//p:RelOp[contains(@PhysicalOp, "Index Spool")])') AS has_index_spool_operators,
            Batch.x.exist('(//p:RelOp[contains(@PhysicalOp, "Table Scan")])') AS has_table_scan_on_heap,
            Batch.x.exist('(//p:RelOp[contains(@PhysicalOp, "Table-valued function")])') AS has_table_valued_functions,
            Batch.x.exist('(//p:UserDefinedFunction)') AS has_user_defined_function,
            Batch.x.exist('(//p:RelOp/@Partitioned[.="1"])') AS has_partitioned_tables,
            CASE 
              WHEN Batch.x.exist('(//p:Aggregate[@AggType="MIN" or @AggType="MAX"])') = 1 THEN 1
              WHEN Batch.x.exist('(//p:TopSort[@Rows="1"])') = 1 THEN 1
              ELSE 0
            END AS has_min_max_agg,

            Batch.x.exist('(//p:NestedLoops[@WithUnorderedPrefetch])') AS is_prefetch_enabled,
        
            /* Return true if it find a large percent of variance on number of returned rows */
            CASE
              WHEN (min_returned_rows + max_returned_rows) / 2 >= @parameter_sniffing_rows_threshold 
                AND min_returned_rows < ((1.0 - (@parameter_sniffing_warning_pct / 100.0)) * avg_returned_rows) THEN 1
              WHEN (min_returned_rows + max_returned_rows) / 2 >= @parameter_sniffing_rows_threshold 
                AND max_returned_rows > ((1.0 + (@parameter_sniffing_warning_pct / 100.0)) * avg_returned_rows) THEN 1
              ELSE 0
            END AS has_parameter_sniffing_problem,

		          CASE 
              WHEN Batch.x.exist('(//p:ParameterList)') = 1 THEN 1
              ELSE 0
            END AS is_parameterized,
            CASE WHEN t_index_list.index_list LIKE '%@%' THEN 1 ELSE 0 END is_using_table_variable,
            total_elapsed_time_sec,
            elapsed_time_sec_percent_over_total,
            avg_elapsed_time_sec,
            min_elapsed_time_sec,
            max_elapsed_time_sec,
            last_elapsed_time_sec,
            total_cpu_time_sec,
            cpu_time_sec_percent_over_total,
            avg_cpu_time_sec,
            min_cpu_time_sec,
            max_cpu_time_sec,
            last_cpu_time_sec,
            total_logical_page_reads,
            logical_page_reads_percent_over_total,
            avg_logical_page_reads,
            min_logical_page_reads,
            max_logical_page_reads,
            last_logical_page_reads,
            total_logical_reads_gb,
            logical_reads_gb_percent_over_total,
            avg_logical_reads_gb,
            min_logical_reads_gb,
            max_logical_reads_gb,
            last_logical_reads_gb,
            total_physical_page_reads,
            physical_page_reads_percent_over_total,
            avg_physical_page_reads,
            min_physical_page_reads,
            max_physical_page_reads,
            last_physical_page_reads,
            total_physical_reads_gb,
            physical_reads_gb_percent_over_total,
            avg_physical_reads_gb,
            min_physical_reads_gb,
            max_physical_reads_gb,
            last_physical_reads_gb,
            total_logical_page_writes,
            logical_page_writes_percent_over_total,
            avglogical_page_writes,
            min_logical_page_writes,
            max_logical_page_writes,
            last_logical_page_writes,
            total_logical_writes_gb,
            logical_writes_gb_percent_over_total,
            avg_logical_writes_gb,
            min_logical_writes_gb,
            max_logical_writes_gb,
            last_logical_writes_gb,
            total_returned_rows,
            avg_returned_rows,
            min_returned_rows,
            max_returned_rows,
            last_returned_rows
    FROM #tmpdm_exec_query_stats AS qp
    OUTER APPLY statement_plan.nodes('//p:Batch') AS Batch(x)
    OUTER APPLY 
      --Get a comma-delimited list of indexes
      (SELECT index_list = STUFF((SELECT DISTINCT ', ' + '(' +
                                         ISNULL(t_index_nodes.col_index.value('(@Database)[1]','sysname') + '.','') + 
                                         ISNULL(t_index_nodes.col_index.value('(@Schema)[1]','sysname') + '.', '') +
                                         t_index_nodes.col_index.value('(@Table)[1]','sysname')  +
                                         ISNULL('.' + t_index_nodes.col_index.value('(@Index)[1]','sysname'),'') + ')'
                                  FROM Batch.x.nodes('//p:Object') t_index_nodes(col_index)
                                  FOR XML PATH(''))
                                , 1, 2,'')
      ) t_index_list
    OUTER APPLY 
      --Get a comma-delimited list of stats
      (SELECT stats_list = STUFF((SELECT DISTINCT ', ' + '(' +
                                        t_stats_nodes.col_stats.value('(@Database)[1]','sysname') + '.' +
                                        t_stats_nodes.col_stats.value('(@Schema)[1]','sysname') + '.' +
                                        t_stats_nodes.col_stats.value('(@Table)[1]','sysname')  +
                                         ISNULL('.' + t_stats_nodes.col_stats.value('(@Statistics)[1]','sysname'),'') + ')'
                                  FROM Batch.x.nodes('//p:OptimizerStatsUsage/p:StatisticsInfo') t_stats_nodes(col_stats)
                                  FOR XML PATH(''))
                                , 1, 2,'')
      ) t_stats_list
    WHERE qp.plan_handle = @plan_handle
    AND qp.statement_start_offset = @statement_start_offset
    AND qp.statement_end_offset = @statement_end_offset
		END TRY
		BEGIN CATCH
			 SELECT @statusMsg = '[' + CONVERT(NVARCHAR(200), GETDATE(), 120) + '] - ' + 'Error trying to work on plan [' + CONVERT(NVARCHAR(800), @plan_handle, 1) + ']. Skipping this plan.'
    RAISERROR (@statusMsg, 0, 0) WITH NOWAIT
    SELECT @statusMsg = '[' + CONVERT(NVARCHAR(200), GETDATE(), 120) + '] - ' + ERROR_MESSAGE() 
    RAISERROR (@statusMsg, 0, 0) WITH NOWAIT
		END CATCH

  SET @i = @i + 1
  FETCH NEXT FROM c_plans
  INTO @query_hash, @plan_handle, @statement_start_offset, @statement_end_offset
END
CLOSE c_plans
DEALLOCATE c_plans

SELECT @statusMsg = '[' + CONVERT(NVARCHAR(200), GETDATE(), 120) + '] - ' + 'Finished to run final query and parse query plan XML and populate tmpIndexCheckCachePlanData'
RAISERROR (@statusMsg, 0, 0) WITH NOWAIT

SET @statusMsg = '[' + CONVERT(VARCHAR(200), GETDATE(), 120) + '] - ' + 'Finished to collect cache plan info...'
RAISERROR(@statusMsg, 0, 42) WITH NOWAIT;

IF OBJECT_ID('tempdb.dbo.Tab_GetIndexInfo') IS NOT NULL
  DROP TABLE tempdb.dbo.Tab_GetIndexInfo

CREATE TABLE tempdb.dbo.Tab_GetIndexInfo
(
  Database_ID INT,
  [Database_Name] [nvarchar] (128) NULL,
  [Schema_Name] [sys].[sysname] NOT NULL,
  [Table_Name] [sys].[sysname] NOT NULL,
  [Index_Name] [sys].[sysname] NULL,
  [File_Group] NVARCHAR(MAX) NULL,
  [Object_ID] INT,
  [Index_ID] INT,
  [Index_Type] [nvarchar] (60) NULL,
  TableHasLOB BIT,
  [Number_Rows] [bigint] NULL,
  [ReservedSizeInMB] [decimal] (18, 2) NULL,
  [reserved_page_count] [bigint] NULL,
  [used_page_count] [bigint] NULL,
  [in_row_data_page_count] [bigint] NULL,
  [in_row_reserved_page_count] [bigint] NULL,
  [lob_reserved_page_count] [bigint] NULL,
  [row_overflow_reserved_page_count] [bigint] NULL,
  [Number_Of_Indexes_On_Table] [int] NULL,
  [avg_fragmentation_in_percent] NUMERIC(25,2) NULL,
  [fragment_count] [bigint] NULL,
  [avg_fragment_size_in_pages] NUMERIC(25,2) NULL,
  [page_count] [bigint] NULL,
  [avg_page_space_used_in_percent] NUMERIC(25,2) NULL,
  [record_count] [bigint] NULL,
  [ghost_record_count] [bigint] NULL,
  [min_record_size_in_bytes] [int] NULL,
  [max_record_size_in_bytes] [int] NULL,
  [avg_record_size_in_bytes] NUMERIC(25,2) NULL,
  [forwarded_record_count] [bigint] NULL,
  [compressed_page_count] [bigint] NULL,
  [fill_factor] [tinyint] NOT NULL,
  [Buffer_Pool_SpaceUsed_MB] [decimal] (18, 2) NOT NULL,
  [Buffer_Pool_FreeSpace_MB] [decimal] (18, 2) NOT NULL,
  [DMV_Missing_Index_Identified] [varchar] (1) NOT NULL,
  [Number_of_missing_index_plans_DMV] [int] NULL,
  [Total Writes] [bigint] NULL,
  [Number_of_Reads] [bigint] NULL,
  [Index_was_never_used] [varchar] (1) NOT NULL,
  [indexed_columns] [xml] NULL,
  key_column_name [sys].[sysname],
  key_column_data_type NVARCHAR(250),
  [included_columns] [xml] NULL,
  [is_unique] [bit] NULL,
  [ignore_dup_key] [bit] NULL,
  [is_primary_key] [bit] NULL,
  [is_unique_constraint] [bit] NULL,
  [is_padded] [bit] NULL,
  [is_disabled] [bit] NULL,
  [is_hypothetical] [bit] NULL,
  [allow_row_locks] [bit] NULL,
  [allow_page_locks] [bit] NULL,
  [has_filter] [bit] NULL,
  [filter_definition] [nvarchar] (max) NULL,
  [create_date] [datetime] NOT NULL,
  [modify_date] [datetime] NOT NULL,
  [uses_ansi_nulls] [bit] NULL,
  [is_replicated] [bit] NULL,
  [has_replication_filter] [bit] NULL,
  [text_in_row_limit] [int] NULL,
  [large_value_types_out_of_row] [bit] NULL,
  [is_tracked_by_cdc] [bit] NULL,
  [lock_escalation_desc] [nvarchar] (60) NULL,
  [partition_number] [nvarchar] (MAX) NOT NULL,
  [data_compression_desc] [nvarchar] (MAX) NULL,
  [user_seeks] [bigint] NULL,
  [user_scans] [bigint] NULL,
  [user_lookups] [bigint] NULL,
  [user_updates] [bigint] NULL,
  [last_user_seek] [datetime] NULL,
  [last_user_scan] [datetime] NULL,
  [last_user_lookup] [datetime] NULL,
  [last_user_update] [datetime] NULL,
  [leaf_insert_count] [bigint] NULL,
  [leaf_delete_count] [bigint] NULL,
  [leaf_update_count] [bigint] NULL,
  [leaf_ghost_count] [bigint] NULL,
  [nonleaf_insert_count] [bigint] NULL,
  [nonleaf_delete_count] [bigint] NULL,
  [nonleaf_update_count] [bigint] NULL,
  [leaf_allocation_count] [bigint] NULL,
  [nonleaf_allocation_count] [bigint] NULL,
  [leaf_page_merge_count] [bigint] NULL,
  [nonleaf_page_merge_count] [bigint] NULL,
  [range_scan_count] [bigint] NULL,
  [singleton_lookup_count] [bigint] NULL,
  [forwarded_fetch_count] [bigint] NULL,
  [lob_fetch_in_pages] [bigint] NULL,
  [lob_fetch_in_bytes] [bigint] NULL,
  [lob_orphan_create_count] [bigint] NULL,
  [lob_orphan_insert_count] [bigint] NULL,
  [row_overflow_fetch_in_pages] [bigint] NULL,
  [row_overflow_fetch_in_bytes] [bigint] NULL,
  [column_value_push_off_row_count] [bigint] NULL,
  [column_value_pull_in_row_count] [bigint] NULL,
  [row_lock_count] [bigint] NULL,
  [row_lock_wait_count] [bigint] NULL,
  [row_lock_wait_in_ms] [bigint] NULL,
  [page_lock_count] [bigint] NULL,
  [page_lock_wait_count] [bigint] NULL,
  [page_lock_wait_in_ms] [bigint] NULL,
  [index_lock_escaltion_attempt_count] [bigint] NULL,
  [index_lock_escaltion_count] [bigint] NULL,
  [page_latch_wait_count] [bigint] NULL,
  [page_latch_wait_in_ms] [bigint] NULL,
  [page_io_latch_wait_count] [bigint] NULL,
  [page_io_latch_wait_in_ms] [bigint] NULL,
  [tree_page_latch_wait_count] [bigint] NULL,
  [tree_page_latch_wait_in_ms] [bigint] NULL,
  [tree_page_io_latch_wait_count] [bigint] NULL,
  [tree_page_io_latch_wait_in_ms] [bigint] NULL,
  [KeyCols_data_length_bytes] INT,
  Key_has_GUID INT,
  IsTablePartitioned BIT,
  IsIndexPartitioned BIT,
  last_datetime_obj_was_used DATETIME,
  plan_cache_reference_count INT
)

  DECLARE @sqlmajorver INT
  SET @sqlmajorver = CONVERT(int, (@@microsoftversion / 0x1000000) & 0xff)

  /*
    Creating list of DBs we'll collect the information
  */
		SELECT @statusMsg = '[' + CONVERT(NVARCHAR(200), GETDATE(), 120) + '] - ' + 'Creating list of databases to work on.'
  RAISERROR (@statusMsg, 0, 0) WITH NOWAIT

  IF OBJECT_ID('tempdb.dbo.#tmp_db') IS NOT NULL
    DROP TABLE #tmp_db

  CREATE TABLE #tmp_db ([Database_Name] sysname)

  /* If this is SQL2012+, check AG status */
  IF (@sqlmajorver >= 11 /*SQL2012*/)
  BEGIN    
    BEGIN TRY
      INSERT INTO #tmp_db
      SELECT d1.[name] 
      FROM sys.databases d1
      LEFT JOIN sys.dm_hadr_availability_replica_states hars
      ON d1.replica_id = hars.replica_id
      LEFT JOIN sys.availability_replicas ar
      ON d1.replica_id = ar.replica_id
      WHERE /* Filtering by the specified DB */
      (d1.name = @database_name_filter OR ISNULL(@database_name_filter, '') = '')
      /* I'm not interested to read DBs that are not online :-) */
      AND d1.state_desc = 'ONLINE'
      /* I'm not sure if info about read_only DBs would be useful, I'm ignoring it until someone convince me otherwise. */
      AND d1.is_read_only = 0 
      /* Not interested to read data about Microsoft stuff, those DBs are already tuned by Microsoft experts, so, no need to tune it, right? ;P */
      AND d1.name not in ('tempdb', 'master', 'model', 'msdb') AND d1.is_distributor = 0
      /* If DB is part of AG, check only DBs that allow connections */
      AND (  
           (hars.role_desc = 'PRIMARY' OR hars.role_desc IS NULL)
           OR 
           (hars.role_desc = 'SECONDARY' AND ar.secondary_role_allow_connections_desc IN ('READ_ONLY','ALL'))
          )
		  END TRY
		  BEGIN CATCH
			   SELECT @statusMsg = '[' + CONVERT(NVARCHAR(200), GETDATE(), 120) + '] - ' + 'Error trying to create list of databases.'
      RAISERROR (@statusMsg, 0, 0) WITH NOWAIT
      SELECT @statusMsg = '[' + CONVERT(NVARCHAR(200), GETDATE(), 120) + '] - ' + ERROR_MESSAGE() 
      RAISERROR (@statusMsg, 0, 0) WITH NOWAIT
		  END CATCH
  END
  /* SQL2008R2 doesn't have AG, so, ignoring the AG DMVs */
  ELSE IF (@sqlmajorver <= 10 /*SQL2008R2*/)
  BEGIN    
    BEGIN TRY
      INSERT INTO #tmp_db
      SELECT d1.[name] 
      FROM sys.databases d1
      WHERE /* Filtering by the specified DB */
      (d1.name = @database_name_filter OR ISNULL(@database_name_filter, '') = '')
      /* I'm not interested to read DBs that are not online :-) */
      AND d1.state_desc = 'ONLINE'
      /* I'm not sure if info about read_only DBs would be useful, I'm ignoring it until someone convince me otherwise. */
      AND d1.is_read_only = 0 
      /* Not interested to read data about Microsoft stuff, those DBs are already tuned by Microsoft experts, so, no need to tune it, right? ;P */
      AND d1.name not in ('tempdb', 'master', 'model', 'msdb') AND d1.is_distributor = 0
		  END TRY
		  BEGIN CATCH
			   SELECT @statusMsg = '[' + CONVERT(NVARCHAR(200), GETDATE(), 120) + '] - ' + 'Error trying to create list of databases.'
      RAISERROR (@statusMsg, 0, 0) WITH NOWAIT
      SELECT @statusMsg = '[' + CONVERT(NVARCHAR(200), GETDATE(), 120) + '] - ' + ERROR_MESSAGE() 
      RAISERROR (@statusMsg, 0, 0) WITH NOWAIT
		  END CATCH
  END

DECLARE @SQL VarCHar(MAX)
declare @Database_Name sysname

DECLARE c_databases CURSOR read_only FOR
    SELECT [Database_Name] FROM #tmp_db
OPEN c_databases

FETCH NEXT FROM c_databases
into @Database_Name
WHILE @@FETCH_STATUS = 0
BEGIN
  SET @statusMsg = '[' + CONVERT(VARCHAR(200), GETDATE(), 120) + '] - ' + 'Working on DB - [' + @Database_Name + ']'
  RAISERROR (@statusMsg, 10, 1) WITH NOWAIT

  SET @SQL = 'use [' + @Database_Name + ']; ' + 

  'DECLARE @statusMsg  VARCHAR(MAX) = ''''
  
  SET NOCOUNT ON;
  SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
  SET LOCK_TIMEOUT 1000; /*1 second*/

  IF OBJECT_ID(''tempdb.dbo.#tmp_dm_db_index_usage_stats'') IS NOT NULL
    DROP TABLE #tmp_dm_db_index_usage_stats
  BEGIN TRY
    /* Creating a copy of sys.dm_db_index_usage_stats because this is too slow to access without an index */
    SELECT database_id, object_id, index_id, user_seeks, user_scans, user_lookups, user_updates, last_user_seek, last_user_scan, last_user_lookup, last_user_update
      INTO #tmp_dm_db_index_usage_stats 
      FROM sys.dm_db_index_usage_stats AS ius WITH(NOLOCK)
      WHERE ius.database_id = DB_ID()
  END TRY
  BEGIN CATCH
    SET @statusMsg = ''['' + CONVERT(VARCHAR(200), GETDATE(), 120) + ''] - '' + ''Error while trying to read data from sys.dm_db_index_usage_stats. You may see limited results because of it.''
    RAISERROR (@statusMsg, 0,0) WITH NOWAIT
  END CATCH

  CREATE CLUSTERED INDEX ix1 ON #tmp_dm_db_index_usage_stats (database_id, object_id, index_id)

  IF OBJECT_ID(''tempdb.dbo.#tmp_dm_db_index_operational_stats'') IS NOT NULL
    DROP TABLE #tmp_dm_db_index_operational_stats

  BEGIN TRY
    /* Creating a copy of sys.dm_db_index_operational_stats because this is too slow to access without an index */
    /* Aggregating the results, to have total for all partitions */
    SELECT DB_ID() AS database_id,
           object_id, 
           index_id, 
           SUM(range_scan_count) AS range_scan_count,
           SUM(singleton_lookup_count) AS singleton_lookup_count,
           SUM(page_latch_wait_count) AS page_latch_wait_count,
           SUM(page_io_latch_wait_count) AS page_io_latch_wait_count,
           SUM(leaf_insert_count) AS leaf_insert_count,
           SUM(leaf_delete_count) AS leaf_delete_count,
           SUM(leaf_update_count) AS leaf_update_count,
           SUM(forwarded_fetch_count) AS forwarded_fetch_count,
           SUM(page_latch_wait_in_ms) AS page_latch_wait_in_ms,
           SUM(leaf_ghost_count) AS leaf_ghost_count,
           SUM(nonleaf_insert_count) AS nonleaf_insert_count,
           SUM(nonleaf_delete_count) AS nonleaf_delete_count,
           SUM(nonleaf_update_count) AS nonleaf_update_count,
           SUM(leaf_allocation_count) AS leaf_allocation_count,
           SUM(nonleaf_allocation_count) AS nonleaf_allocation_count,
           SUM(leaf_page_merge_count) AS leaf_page_merge_count,
           SUM(nonleaf_page_merge_count) AS nonleaf_page_merge_count,
           SUM(lob_fetch_in_pages) AS lob_fetch_in_pages,
           SUM(lob_fetch_in_bytes) AS lob_fetch_in_bytes,
           SUM(lob_orphan_create_count) AS lob_orphan_create_count,
           SUM(lob_orphan_insert_count) AS lob_orphan_insert_count,
           SUM(row_overflow_fetch_in_pages) AS row_overflow_fetch_in_pages,
           SUM(row_overflow_fetch_in_bytes) AS row_overflow_fetch_in_bytes,
           SUM(column_value_push_off_row_count) AS column_value_push_off_row_count,
           SUM(column_value_pull_in_row_count) AS column_value_pull_in_row_count,
           SUM(row_lock_count) AS row_lock_count,
           SUM(row_lock_wait_count) AS row_lock_wait_count,
           SUM(row_lock_wait_in_ms) AS row_lock_wait_in_ms,
           SUM(page_lock_count) AS page_lock_count,
           SUM(page_lock_wait_count) AS page_lock_wait_count,
           SUM(page_lock_wait_in_ms) AS page_lock_wait_in_ms,
           SUM(index_lock_promotion_attempt_count) AS index_lock_promotion_attempt_count,
           SUM(index_lock_promotion_count) AS index_lock_promotion_count,
           SUM(tree_page_latch_wait_count) AS tree_page_latch_wait_count,
           SUM(tree_page_latch_wait_in_ms) AS tree_page_latch_wait_in_ms,
           SUM(tree_page_io_latch_wait_count) AS tree_page_io_latch_wait_count,
           SUM(tree_page_io_latch_wait_in_ms) AS tree_page_io_latch_wait_in_ms,
           CONVERT(NUMERIC(25, 2),
           CASE 
             WHEN SUM(page_latch_wait_count) > 0 THEN SUM(page_latch_wait_in_ms) / (1. * SUM(page_latch_wait_count))
             ELSE 0 
           END) AS avg_page_latch_wait_in_ms,
           CONVERT(VARCHAR(200), (SUM(page_latch_wait_in_ms) / 1000) / 86400) + '':'' + CONVERT(VARCHAR(20), DATEADD(s, (SUM(page_latch_wait_in_ms) / 1000), 0), 108) AS page_latch_wait_time_d_h_m_s,
           SUM(page_io_latch_wait_in_ms) AS page_io_latch_wait_in_ms,
           CONVERT(NUMERIC(25, 2), 
           CASE 
             WHEN SUM(page_io_latch_wait_count) > 0 THEN SUM(page_io_latch_wait_in_ms) / (1. * SUM(page_io_latch_wait_count))
             ELSE 0 
           END) AS avg_page_io_latch_wait_in_ms,
           CONVERT(VARCHAR(200), (SUM(page_io_latch_wait_in_ms) / 1000) / 86400) + '':'' + CONVERT(VARCHAR(20), DATEADD(s, (SUM(page_io_latch_wait_in_ms) / 1000), 0), 108) AS page_io_latch_wait_time_d_h_m_s
      INTO #tmp_dm_db_index_operational_stats
      FROM sys.dm_db_index_operational_stats (DB_ID(), NULL, NULL, NULL) AS ios
     GROUP BY object_id, 
           index_id
     OPTION (MAXDOP 1)
  END TRY
  BEGIN CATCH
    SET @statusMsg = ''['' + CONVERT(VARCHAR(200), GETDATE(), 120) + ''] - '' + ''Error while trying to read data from sys.dm_db_index_operational_stats. You may see limited results because of it.''
    RAISERROR (@statusMsg, 0,0) WITH NOWAIT
  END CATCH

  CREATE CLUSTERED INDEX ix1 ON #tmp_dm_db_index_operational_stats (database_id, object_id, index_id)

  SET @statusMsg = ''['' + CONVERT(VARCHAR(200), GETDATE(), 120) + ''] - '' + ''Creating copy of system tables...''
  RAISERROR(@statusMsg, 0, 42) WITH NOWAIT;

  /* Creating a copy of system tables because unindexed access to it can be very slow */
  IF OBJECT_ID(''tempdb.dbo.#tmp_sys_partitions'') IS NOT NULL
      DROP TABLE #tmp_sys_partitions;
  SELECT * INTO #tmp_sys_partitions FROM sys.partitions
  CREATE CLUSTERED INDEX ix1 ON #tmp_sys_partitions (object_id, index_id, partition_number)

  IF OBJECT_ID(''tempdb.dbo.#tmp_sys_dm_db_partition_stats'') IS NOT NULL
      DROP TABLE #tmp_sys_dm_db_partition_stats;
  SELECT * INTO #tmp_sys_dm_db_partition_stats FROM sys.dm_db_partition_stats
  CREATE CLUSTERED INDEX ix1 ON #tmp_sys_dm_db_partition_stats (object_id, index_id, partition_number)

  IF OBJECT_ID(''tempdb.dbo.#tmp_sys_allocation_units'') IS NOT NULL
      DROP TABLE #tmp_sys_allocation_units;
  SELECT * INTO #tmp_sys_allocation_units FROM sys.allocation_units
  CREATE CLUSTERED INDEX ix1 ON #tmp_sys_allocation_units (container_id)

  IF OBJECT_ID(''tempdb.dbo.#tmp_sys_index_columns'') IS NOT NULL
      DROP TABLE #tmp_sys_index_columns;
  SELECT * INTO #tmp_sys_index_columns FROM sys.index_columns
  CREATE CLUSTERED INDEX ix1 ON #tmp_sys_index_columns (object_id, index_id, index_column_id, column_id)

  IF OBJECT_ID(''tempdb.dbo.#tmp_sys_all_columns'') IS NOT NULL
      DROP TABLE #tmp_sys_all_columns;
  SELECT * INTO #tmp_sys_all_columns FROM sys.all_columns
  CREATE CLUSTERED INDEX ix1 ON #tmp_sys_all_columns (object_id, column_id)

  IF OBJECT_ID(''tempdb.dbo.#tmp_sys_indexes'') IS NOT NULL
      DROP TABLE #tmp_sys_indexes;
  SELECT * INTO #tmp_sys_indexes FROM sys.indexes
  CREATE CLUSTERED INDEX ix1 ON #tmp_sys_indexes (object_id, index_id)

  IF OBJECT_ID(''tempdb.dbo.#tmp_sys_tables'') IS NOT NULL
      DROP TABLE #tmp_sys_tables;
  SELECT * INTO #tmp_sys_tables FROM sys.tables
  CREATE CLUSTERED INDEX ix1 ON #tmp_sys_tables (object_id, schema_id)

  IF OBJECT_ID(''tempdb.dbo.#tmp_sys_objects'') IS NOT NULL
      DROP TABLE #tmp_sys_objects;
  SELECT * INTO #tmp_sys_objects FROM sys.objects
  CREATE CLUSTERED INDEX ix1 ON #tmp_sys_objects (object_id, schema_id)

  IF OBJECT_ID(''tempdb.dbo.#tmp_sys_schemas'') IS NOT NULL
      DROP TABLE #tmp_sys_schemas;
  SELECT * INTO #tmp_sys_schemas FROM sys.schemas
  CREATE CLUSTERED INDEX ix1 ON #tmp_sys_schemas (schema_id)

  IF OBJECT_ID(''tempdb.dbo.#tmp_sys_types'') IS NOT NULL
      DROP TABLE #tmp_sys_types;
  SELECT * INTO #tmp_sys_types FROM sys.types
  CREATE CLUSTERED INDEX ix1 ON #tmp_sys_types (user_type_id)

  IF OBJECT_ID(''tempdb.dbo.#tmp_sys_filegroups'') IS NOT NULL
      DROP TABLE #tmp_sys_filegroups;
  SELECT * INTO #tmp_sys_filegroups FROM sys.filegroups
  CREATE CLUSTERED INDEX ix1 ON #tmp_sys_filegroups (data_space_id)

  IF OBJECT_ID(''tempdb.dbo.#tmp_sys_dm_db_missing_index_details'') IS NOT NULL
      DROP TABLE #tmp_sys_dm_db_missing_index_details;
  SELECT * INTO #tmp_sys_dm_db_missing_index_details FROM sys.dm_db_missing_index_details
  WHERE database_id = DB_ID()
  CREATE CLUSTERED INDEX ix1 ON #tmp_sys_dm_db_missing_index_details (database_id, object_id)

  SET @statusMsg = ''['' + CONVERT(VARCHAR(200), GETDATE(), 120) + ''] - '' + ''Finished to create copy of system tables...''
  RAISERROR(@statusMsg, 0, 42) WITH NOWAIT;

  SET @statusMsg = ''['' + CONVERT(VARCHAR(200), GETDATE(), 120) + ''] - '' + ''Collecting index fragmentation info...''
  RAISERROR(@statusMsg, 0, 42) WITH NOWAIT;

  SET LOCK_TIMEOUT 5000; /*5 seconds*/
  DECLARE @objname sysname, @idxname sysname, @object_id INT, @index_id INT, @row_count VARCHAR(50), @tot INT, @i INT, @size_gb NUMERIC(36, 4)

  IF OBJECT_ID(''tempdb.dbo.#tmpIndexFrag'') IS NOT NULL
    DROP TABLE #tmpIndexFrag;

  CREATE TABLE [#tmpIndexFrag]
  (
    [database_id] SMALLINT,
    [object_id] INT,
    [index_id] INT,
    [avg_fragmentation_in_percent] NUMERIC(25,2),
    [fragment_count] BIGINT,
    [avg_fragment_size_in_pages] NUMERIC(25,2),
    [page_count] BIGINT,
    [avg_page_space_used_in_percent] NUMERIC(25,2),
    [record_count] BIGINT,
    [ghost_record_count] BIGINT,
    [min_record_size_in_bytes] INT,
    [max_record_size_in_bytes] INT,
    [avg_record_size_in_bytes] NUMERIC(25,2),
    [forwarded_record_count] BIGINT,
    [compressed_page_count] BIGINT
  )

  IF OBJECT_ID(''tempdb.dbo.#tmpIndexFrag_Cursor'') IS NOT NULL
    DROP TABLE #tmpIndexFrag_Cursor;

  DECLARE @TOPFrag INT = 2147483647
  IF OBJECT_ID(''tempdb.dbo.#tmp_skipfrag'') IS NOT NULL
  BEGIN
    SET @TOPFrag = 0
  END

  SELECT TOP (@TOPFrag)
    objects.name AS objname, 
    ISNULL(indexes.name, ''HEAP'') AS idxname, 
    indexes.object_id, 
    indexes.index_id, 
    PARSENAME(CONVERT(VARCHAR(50), CONVERT(MONEY, SUM(dm_db_partition_stats.row_count)), 1), 2) AS row_count,
    CAST(ROUND(SUM(used_page_count) * 8 / 1024.00 / 1024., 2) AS NUMERIC(36, 4)) AS size_gb
  INTO #tmpIndexFrag_Cursor
  FROM #tmp_sys_indexes AS indexes
  INNER JOIN #tmp_sys_objects AS objects
  ON objects.object_id = indexes.object_id
  INNER JOIN #tmp_sys_dm_db_partition_stats AS dm_db_partition_stats
  ON dm_db_partition_stats.object_id = indexes.object_id
  AND dm_db_partition_stats.index_id = indexes.index_id
  WHERE objects.type = ''U''
  AND indexes.type not in (5, 6) /*ignoring columnstore indexes*/
  GROUP BY objects.name, 
           ISNULL(indexes.name, ''HEAP''), 
           indexes.object_id,
           indexes.index_id
  HAVING SUM(dm_db_partition_stats.row_count) > 0

  SET @tot = @@ROWCOUNT

  DECLARE c_allrows CURSOR READ_ONLY FOR
  SELECT * FROM #tmpIndexFrag_Cursor
  ORDER BY row_count ASC

  OPEN c_allrows

  FETCH NEXT FROM c_allrows
  INTO @objname, @idxname, @object_id, @index_id, @row_count, @size_gb

  SET @i = 0
  WHILE @@FETCH_STATUS = 0
  BEGIN
    SET @i = @i + 1

    SET @statusMsg = ''['' + CONVERT(VARCHAR(200), GETDATE(), 120) + ''] - '' + ''Working on index '' + CONVERT(VARCHAR, @i) + '' of '' + CONVERT(VARCHAR, @tot) + '': ObjName = '' + QUOTENAME(@objname) + '' | IndexName = '' + QUOTENAME(@idxname) + '' | RowCount = '' + @row_count + '' | Size_gb = '' + CONVERT(VARCHAR, @size_gb)
    RAISERROR (@statusMsg, 10, 1) WITH NOWAIT

    BEGIN TRY
      IF @size_gb >= 5.0000
      BEGIN
        SET @statusMsg = ''['' + CONVERT(VARCHAR(200), GETDATE(), 120) + ''] - '' + ''Warning - Index is too big (>=5gb), skipping collection data about this table/index.''
        RAISERROR (@statusMsg, 10, 1) WITH NOWAIT
      END
      ELSE
      BEGIN
        INSERT INTO #tmpIndexFrag
        SELECT
          dm_db_index_physical_stats.database_id,
          dm_db_index_physical_stats.object_id,
          dm_db_index_physical_stats.index_id,
          AVG(ISNULL(dm_db_index_physical_stats.avg_fragmentation_in_percent,0)) AS avg_fragmentation_in_percent,
          SUM(ISNULL(dm_db_index_physical_stats.fragment_count,0)) AS fragment_count,
          AVG(ISNULL(dm_db_index_physical_stats.avg_fragment_size_in_pages,0)) AS avg_fragment_size_in_pages,
          SUM(ISNULL(dm_db_index_physical_stats.page_count,0)) AS page_count,
          AVG(ISNULL(dm_db_index_physical_stats.avg_page_space_used_in_percent,0)) AS avg_page_space_used_in_percent,
          SUM(ISNULL(dm_db_index_physical_stats.record_count,0)) AS record_count,
          SUM(ISNULL(dm_db_index_physical_stats.ghost_record_count,0)) AS ghost_record_count,
          MIN(ISNULL(dm_db_index_physical_stats.min_record_size_in_bytes,0)) AS min_record_size_in_bytes,
          MAX(ISNULL(dm_db_index_physical_stats.max_record_size_in_bytes,0)) AS max_record_size_in_bytes,
          AVG(ISNULL(dm_db_index_physical_stats.avg_record_size_in_bytes,0)) AS avg_record_size_in_bytes,
          SUM(ISNULL(dm_db_index_physical_stats.forwarded_record_count,0)) AS forwarded_record_count,
          SUM(ISNULL(dm_db_index_physical_stats.compressed_page_count,0)) AS compressed_page_count
        FROM sys.dm_db_index_physical_stats(DB_ID(), @object_id, @index_id, NULL, CASE WHEN @size_gb >= 1.00 THEN ''LIMITED'' ELSE ''SAMPLED'' END)
        WHERE index_level = 0 /*leaf-level nodes only*/
        GROUP BY dm_db_index_physical_stats.database_id,
                 dm_db_index_physical_stats.object_id,
                 dm_db_index_physical_stats.index_id
        OPTION (RECOMPILE);
      END
    END TRY
    BEGIN CATCH
      SET @statusMsg = ''['' + CONVERT(VARCHAR(200), GETDATE(), 120) + ''] - '' + ''Error trying to run index fragmentation query... Timeout... Skipping collection data about this table/index.''
      RAISERROR (@statusMsg, 10, 1) WITH NOWAIT
    END CATCH

    FETCH NEXT FROM c_allrows
    INTO @objname, @idxname, @object_id, @index_id, @row_count, @size_gb
  END
  CLOSE c_allrows
  DEALLOCATE c_allrows

  SET @statusMsg = ''['' + CONVERT(VARCHAR(200), GETDATE(), 120) + ''] - '' + ''Finished to collect index fragmentation info...''
  RAISERROR(@statusMsg, 0, 42) WITH NOWAIT;

  SELECT DB_ID() AS database_id,
         DB_NAME() AS ''Database_Name'',
         sc.name AS ''Schema_Name'',
         t.name AS ''Table_Name'',
         i.name AS ''Index_Name'',
         fg.name AS ''File_Group'',
         t.object_id,
         i.index_id,
         i.type_desc AS ''Index_Type'',
         ISNULL(t1.TableHasLOB, 0) AS TableHasLOB,
         tSize.row_count AS ''Number_Rows'',
         tSize.ReservedSizeInMB,
         tSize.reserved_page_count,
         tSize.used_page_count,
         tSize.in_row_data_page_count,
         tSize.in_row_reserved_page_count,
         tSize.lob_reserved_page_count,
         tSize.row_overflow_reserved_page_count,
         tNumerOfIndexes.Cnt AS ''Number_Of_Indexes_On_Table'',
         #tmpIndexFrag.avg_fragmentation_in_percent,
         #tmpIndexFrag.fragment_count,
         #tmpIndexFrag.avg_fragment_size_in_pages,
         #tmpIndexFrag.page_count,
         #tmpIndexFrag.avg_page_space_used_in_percent,
         #tmpIndexFrag.record_count,
         #tmpIndexFrag.ghost_record_count,
         #tmpIndexFrag.min_record_size_in_bytes,
         #tmpIndexFrag.max_record_size_in_bytes,
         #tmpIndexFrag.avg_record_size_in_bytes,
         #tmpIndexFrag.forwarded_record_count,
         #tmpIndexFrag.compressed_page_count,
         i.fill_factor,
         ISNULL(bp.CacheSizeMB, 0) AS ''Buffer_Pool_SpaceUsed_MB'',
         ISNULL(bp.FreeSpaceMB, 0) AS ''Buffer_Pool_FreeSpace_MB'',
         CASE
             WHEN mid.database_id IS NULL THEN
                 ''N''
             ELSE
                 ''Y''
         END AS ''DMV_Missing_Index_Identified'',
         mid.Number_of_missing_index_plans_DMV,
         ius.user_updates AS [Total Writes], 
         ius.user_seeks + ius.user_scans + ius.user_lookups AS ''Number_of_Reads'',
         CASE
             WHEN ius.user_seeks + ius.user_scans + ius.user_lookups = 0 THEN
                 ''Y''
             ELSE
                 ''N''
         END AS ''Index_was_never_used'',
         CONVERT(XML, ISNULL(REPLACE(REPLACE(REPLACE(
                                (
                                    SELECT QUOTENAME(c.name, ''"'') AS ''columnName''
                                    FROM #tmp_sys_index_columns AS sic
                                        JOIN #tmp_sys_all_columns AS c
                                            ON c.column_id = sic.column_id
                                               AND c.object_id = sic.object_id
                                    WHERE sic.object_id = i.object_id
                                          AND sic.index_id = i.index_id
                                          AND is_included_column = 0
                                    ORDER BY sic.index_column_id
                                    FOR XML RAW
                                ),
                                ''"/><row columnName="'',
                                '', ''
                                       ),
                                ''<row columnName="'',
                                ''''
                               ),
                        ''"/>'',
                        ''''
                       ),
                ''''
               )) AS ''indexed_columns'',
         ISNULL(tab_index_key_column.indexkeycolumnname, '''') AS key_column_name,
         ISNULL(tab_index_key_column.keycolumndatatype, '''') AS key_column_data_type,
         CONVERT(XML, ISNULL(REPLACE(REPLACE(REPLACE(
                                (
                                    SELECT QUOTENAME(c.name, ''"'') AS ''columnName''
                                    FROM #tmp_sys_index_columns AS sic
                                        JOIN #tmp_sys_all_columns AS c
                                            ON c.column_id = sic.column_id
                                               AND c.object_id = sic.object_id
                                    WHERE sic.object_id = i.object_id
                                          AND sic.index_id = i.index_id
                                          AND is_included_column = 1
                                    ORDER BY sic.index_column_id
                                    FOR XML RAW
                                ),
                                ''"/><row columnName="'',
                                '', ''
                                       ),
                                ''<row columnName="'',
                                ''''
                               ),
                        ''"/>'',
                        ''''
                       ),
                ''''
               )) AS ''included_columns'',
         i.is_unique,
         i.ignore_dup_key,
         i.is_primary_key,
         i.is_unique_constraint,
         i.is_padded,
         i.is_disabled,
         i.is_hypothetical,
         i.allow_row_locks,
         i.allow_page_locks,
         i.has_filter,
         i.filter_definition,
         t.create_date,
         t.modify_date,
         t.uses_ansi_nulls,
         t.is_replicated,
         t.has_replication_filter,
         t.text_in_row_limit,
         t.large_value_types_out_of_row,
         t.is_tracked_by_cdc,
         t.lock_escalation_desc,
         ISNULL(p.partition_number, 1) AS partition_number,
         ISNULL(p.data_compression_desc, '''') AS data_compression_desc,
         ius.user_seeks,
         ius.user_scans,
         ius.user_lookups,
         ius.user_updates,
         ius.last_user_seek,
         ius.last_user_scan,
         ius.last_user_lookup,
         ius.last_user_update,
         ios.leaf_insert_count,
         ios.leaf_delete_count,
         ios.leaf_update_count,
         ios.leaf_ghost_count,
         ios.nonleaf_insert_count,
         ios.nonleaf_delete_count,
         ios.nonleaf_update_count,
         ios.leaf_allocation_count,
         ios.nonleaf_allocation_count,
         ios.leaf_page_merge_count,
         ios.nonleaf_page_merge_count,
         ios.range_scan_count,
         ios.singleton_lookup_count,
         ios.forwarded_fetch_count,
         ios.lob_fetch_in_pages,
         ios.lob_fetch_in_bytes,
         ios.lob_orphan_create_count,
         ios.lob_orphan_insert_count,
         ios.row_overflow_fetch_in_pages,
         ios.row_overflow_fetch_in_bytes,
         ios.column_value_push_off_row_count,
         ios.column_value_pull_in_row_count,
         ios.row_lock_count,
         ios.row_lock_wait_count,
         ios.row_lock_wait_in_ms,
         ios.page_lock_count,
         ios.page_lock_wait_count,
         ios.page_lock_wait_in_ms,
         ios.index_lock_promotion_attempt_count AS index_lock_escaltion_attempt_count,
         ios.index_lock_promotion_count AS index_lock_escaltion_count,
         ios.page_latch_wait_count,
         ios.page_latch_wait_in_ms,
         ios.page_io_latch_wait_count,
         ios.page_io_latch_wait_in_ms,
         ios.tree_page_latch_wait_count,
         ios.tree_page_latch_wait_in_ms,
         ios.tree_page_io_latch_wait_count,
         ios.tree_page_io_latch_wait_in_ms,
         (SELECT SUM(CASE sty.name WHEN ''nvarchar'' THEN sc.max_length/2 ELSE sc.max_length END) 
          FROM #tmp_sys_indexes AS ii
		        INNER JOIN #tmp_sys_tables AS tt ON tt.[object_id] = ii.[object_id]
		        INNER JOIN #tmp_sys_schemas ss ON ss.[schema_id] = tt.[schema_id]
		        INNER JOIN #tmp_sys_index_columns AS sic ON sic.object_id = tt.object_id AND sic.index_id = ii.index_id
		        INNER JOIN #tmp_sys_all_columns AS sc ON sc.object_id = tt.object_id AND sc.column_id = sic.column_id
		        INNER JOIN #tmp_sys_types AS sty ON sc.user_type_id = sty.user_type_id
		        WHERE ii.[object_id] = i.[object_id] 
            AND ii.index_id = i.index_id 
            AND sic.key_ordinal > 0) AS [KeyCols_data_length_bytes],
         (SELECT COUNT(sty.name) 
            FROM #tmp_sys_indexes AS ii
		         INNER JOIN #tmp_sys_tables AS tt ON tt.[object_id] = ii.[object_id]
		         INNER JOIN #tmp_sys_schemas ss ON ss.[schema_id] = tt.[schema_id]
		         INNER JOIN #tmp_sys_index_columns AS sic ON sic.object_id = i.object_id AND sic.index_id = i.index_id
		         INNER JOIN #tmp_sys_all_columns AS sc ON sc.object_id = tt.object_id AND sc.column_id = sic.column_id
		         INNER JOIN #tmp_sys_types AS sty ON sc.user_type_id = sty.user_type_id
		         WHERE i.[object_id] = ii.[object_id] 
           AND i.index_id = ii.index_id 
           AND sic.is_included_column = 0 
           AND sty.name = ''uniqueidentifier'') AS [Key_has_GUID],
         CASE 
           WHEN EXISTS(SELECT *
                         FROM #tmp_sys_partitions pp
                        WHERE pp.partition_number > 1
                          AND pp.object_id = i.object_Id
                          AND pp.index_id IN (0, 1)) THEN 1
           ELSE 0
         END AS IsTablePartitioned,
         CASE 
           WHEN EXISTS(SELECT *
                         FROM #tmp_sys_partitions pp
                        WHERE pp.partition_number > 1
                          AND pp.object_id = i.object_Id
                          AND pp.index_id = i.index_id) THEN 1
           ELSE 0
         END AS IsIndexPartitioned,
         TabIndexUsage.last_datetime_obj_was_used,
         0 AS plan_cache_reference_count
  FROM #tmp_sys_indexes i WITH (NOLOCK)
      INNER JOIN #tmp_sys_tables t
          ON t.object_id = i.object_id
      INNER JOIN #tmp_sys_schemas sc WITH (NOLOCK)
          ON sc.schema_id = t.schema_id
      CROSS APPLY
      (
         SELECT STUFF((SELECT '','' + CONVERT(VARCHAR, #tmp_sys_partitions.partition_number)
                       FROM #tmp_sys_partitions
                       WHERE #tmp_sys_partitions.object_id = i.object_id
                       AND #tmp_sys_partitions.index_id = i.index_id
                       ORDER BY #tmp_sys_partitions.partition_number
                       FOR XML PATH('''')), 1, 1, ''''),
                STUFF((SELECT '','' + CONVERT(VARCHAR, #tmp_sys_partitions.partition_number) + ''('' + CONVERT(VARCHAR(200), #tmp_sys_partitions.data_compression_desc) + '')''
                              FROM #tmp_sys_partitions
                              WHERE #tmp_sys_partitions.object_id = i.object_id
                              AND #tmp_sys_partitions.index_id = i.index_id
                              ORDER BY #tmp_sys_partitions.partition_number
                              FOR XML PATH('''')), 1, 1, '''')
      ) AS p (partition_number, data_compression_desc)
      OUTER APPLY(
        SELECT SUM(bp.CacheSizeMB) AS CacheSizeMB,
               SUM(bp.FreeSpaceMB) AS FreeSpaceMB
        FROM #tmp_sys_partitions AS p
        INNER JOIN #tmp_sys_allocation_units AS au
            ON au.container_id = p.hobt_id
        LEFT OUTER JOIN #tmpBufferDescriptors AS bp
            ON bp.database_id = DB_ID()
           AND bp.allocation_unit_id = au.allocation_unit_id
        WHERE p.object_id = i.object_id
        AND p.index_id = i.index_id
      ) AS bp
      CROSS APPLY (
        SELECT STUFF((SELECT DISTINCT '','' + CONVERT(VARCHAR, p.partition_number) + ''('' + CONVERT(VARCHAR(200), au.type_desc COLLATE Latin1_General_Bin2) + '', '' + CONVERT(VARCHAR(200), fg.name COLLATE Latin1_General_Bin2) + '')''
                               FROM #tmp_sys_partitions AS p
          INNER JOIN #tmp_sys_allocation_units AS au
          ON au.container_id = p.hobt_id
          INNER JOIN #tmp_sys_filegroups AS fg
          ON fg.data_space_id = au.data_space_id
          WHERE p.object_id = i.object_id
          AND p.index_id = i.index_id
          FOR XML PATH('''')), 1, 1, '''')
      ) AS fg (name)
      OUTER APPLY (SELECT TOP 1 
                          1 AS TableHasLOB
                          FROM #tmp_sys_partitions AS p
                          INNER JOIN #tmp_sys_allocation_units AS au
                          ON au.container_id = p.hobt_id
                          WHERE p.object_id = i.object_id
                          AND au.type = 2 /*Type of allocation unit: 2 = Large object (LOB) data (text, ntext, image, xml, large value types, and CLR user-defined types)*/
      ) as t1
      CROSS APPLY
      (
          SELECT CONVERT(DECIMAL(18, 2), SUM((st.reserved_page_count * 8) / 1024.)) ReservedSizeInMB,
                 SUM(st.reserved_page_count) AS reserved_page_count,
                 SUM(st.used_page_count) AS used_page_count,
                 SUM(st.in_row_data_page_count) AS in_row_data_page_count,
                 SUM(st.in_row_reserved_page_count) AS in_row_reserved_page_count,
                 SUM(st.lob_reserved_page_count) AS lob_reserved_page_count,
                 SUM(st.row_overflow_reserved_page_count) AS row_overflow_reserved_page_count,
                 ISNULL(SUM(st.row_count), (SELECT SUM(p1.row_count) FROM #tmp_sys_dm_db_partition_stats as p1 WHERE i.object_id = p1.object_id AND p1.index_id <= 1)) AS row_count
          FROM #tmp_sys_dm_db_partition_stats st
          WHERE i.object_id = st.object_id
                AND i.index_id = st.index_id
      ) AS tSize
      LEFT OUTER JOIN #tmp_dm_db_index_usage_stats ius WITH (NOLOCK)
          ON ius.index_id = i.index_id
             AND ius.object_id = i.object_id
             AND ius.database_id = DB_ID()
      LEFT OUTER JOIN #tmp_dm_db_index_operational_stats AS ios WITH (NOLOCK)
          ON ios.database_id = DB_ID()
         AND ios.object_id = i.object_id
         AND ios.index_id = i.index_id
      LEFT OUTER JOIN #tmpIndexFrag
          ON i.object_id = #tmpIndexFrag.object_id
             AND i.index_id = #tmpIndexFrag.index_id
             AND #tmpIndexFrag.database_id = DB_ID()
      LEFT OUTER JOIN
      (
          SELECT database_id,
                 object_id,
                 COUNT(*) AS Number_of_missing_index_plans_DMV
          FROM #tmp_sys_dm_db_missing_index_details
          GROUP BY database_id,
                   object_id
      ) AS mid
          ON mid.database_id = DB_ID()
             AND mid.object_id = i.object_id
      CROSS APPLY
      (
          SELECT COUNT(*) AS Cnt
          FROM #tmp_sys_indexes i1
          WHERE i.object_id = i1.object_id
          AND i1.index_id <> 0 /*ignoring heaps*/
      ) AS tNumerOfIndexes
      OUTER APPLY (SELECT all_columns.Name AS indexkeycolumnname, 
                           CASE 
                             WHEN COLUMNPROPERTY(all_columns.object_id, all_columns.name, ''Precision'') = -1 THEN 1
                             WHEN COLUMNPROPERTY(all_columns.object_id, all_columns.name, ''Precision'') = 2147483647 THEN 1
                             ELSE 0
                           END AS islob,
                           UPPER(TYPE_NAME(types.system_type_id)) + '' (precision = '' + 
                           CONVERT(VARCHAR(20), COLUMNPROPERTY(all_columns.object_id, all_columns.name, ''Precision'')) + 
                           '', scale = '' +
                           ISNULL(CONVERT(VARCHAR(20), COLUMNPROPERTY(all_columns.object_id, all_columns.name, ''Scale'')), ''0'') + 
                           '')'' AS keycolumndatatype
                   FROM #tmp_sys_index_columns AS index_columns
                   INNER JOIN #tmp_sys_all_columns AS all_columns
                   ON all_columns.object_id = index_columns.object_id
                   AND all_columns.column_id = index_columns.column_id
                   INNER JOIN #tmp_sys_types AS types
                   ON types.user_type_id = all_columns.user_type_id
                   WHERE i.object_id = index_columns.object_id
                   AND i.index_id = index_columns.index_id
                   AND index_columns.key_ordinal = 1
                   AND index_columns.is_included_column = 0) AS tab_index_key_column
       OUTER APPLY (SELECT MAX(Dt) FROM (VALUES(ius.last_user_seek), 
                                               (ius.last_user_scan),
                                               (ius.last_user_lookup)
                                      ) AS t(Dt)) AS TabIndexUsage(last_datetime_obj_was_used)
  WHERE OBJECTPROPERTY(i.[object_id], ''IsUserTable'') = 1
  ORDER BY tSize.ReservedSizeInMB DESC
  '

  /*
    SELECT @SQL
  */  
  
  INSERT INTO tempdb.dbo.Tab_GetIndexInfo
  EXEC (@SQL)
  
  FETCH NEXT FROM c_databases
  into @Database_Name
END
CLOSE c_databases
DEALLOCATE c_databases

SELECT @statusMsg = '[' + CONVERT(NVARCHAR(200), GETDATE(), 120) + '] - ' + 'Updating plan_cache_reference_count column.'
RAISERROR (@statusMsg, 0, 0) WITH NOWAIT

UPDATE tempdb.dbo.Tab_GetIndexInfo 
SET plan_cache_reference_count = (SELECT COUNT(DISTINCT query_hash) 
                                    FROM tempdb.dbo.tmpIndexCheckCachePlanData
                                   WHERE CONVERT(NVARCHAR(MAX), tmpIndexCheckCachePlanData.index_list) COLLATE Latin1_General_BIN2 LIKE '%' + REPLACE(REPLACE(Tab1.Col1,'[','!['),']','!]') + '%' ESCAPE '!')
FROM tempdb.dbo.Tab_GetIndexInfo
CROSS APPLY (SELECT '(' + QUOTENAME(Database_Name) + '.' + 
                          QUOTENAME(Schema_Name) + '.' + 
                          QUOTENAME(Table_Name) + 
                          ISNULL('.' + QUOTENAME(Index_Name),'') + ')') AS Tab1(Col1)

SELECT @statusMsg = '[' + CONVERT(NVARCHAR(200), GETDATE(), 120) + '] - ' + 'Finished to update plan_cache_reference_count column.'
RAISERROR (@statusMsg, 0, 0) WITH NOWAIT

CREATE UNIQUE CLUSTERED INDEX ix1 ON tempdb.dbo.Tab_GetIndexInfo(Database_ID, Object_ID, Index_ID)
CREATE INDEX ix2 ON tempdb.dbo.Tab_GetIndexInfo(Database_Name, Schema_Name, Table_Name) INCLUDE(Index_ID, Number_Rows)

SELECT @statusMsg = '[' + CONVERT(NVARCHAR(200), GETDATE(), 120) + '] - ' + 'Finished to run script.'
RAISERROR (@statusMsg, 0, 0) WITH NOWAIT

--SELECT * FROM tempdb.dbo.Tab_GetIndexInfo
--ORDER BY ReservedSizeInMB DESC
END
GO