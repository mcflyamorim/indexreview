/* 
Check22 – Missing index plan cache

Description:
Potentially missing indexes were found based on SQL Server query plan cache. It is important to revise them.

Estimated Benefit:
Very High

Estimated Effort:
High

Recommendation:
Quick recommendation:
Review recommended missing indexes and if possible, create them.

Detailed recommendation:
Review missing index suggestions to effectively tune indexes and improve query performance. Review the base table structure, carefully combine indexes, consider key column order, and review included column suggestions, examine missing indexes and existing indexes for overlap and avoid creating duplicate indexes.
It's a best practice to review all the missing index requests for a table and the existing indexes on a table before adding an index based on a query execution plan.
Missing index suggestions are best treated as one of several sources of information when performing index analysis, design, tuning, and testing. Missing index suggestions are not prescriptions to create indexes exactly as suggested.
Review the missing index recommendations for a table as a group, along with the definitions of existing indexes on the table. Remember that when defining indexes, generally equality columns should be put before the inequality columns, and together they should form the key of the index. To determine an effective order for the equality columns, order them based on their SELECTivity: list the most SELECTive columns first (leftmost in the column list). Unique columns are most SELECTive, while columns with many repeating values are less SELECTive.
It's important to confirm if your index changes have been successful, “is the query optimizer using your indexes?”. Keep in mind that while indexes can dramatically improve query performance but indexes also have overhead and management costs. 
*/

SET NOCOUNT ON; 
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
SET LOCK_TIMEOUT 60000; /*60 seconds*/
SET DATEFORMAT MDY

IF OBJECT_ID('tempdb.dbo.tmpIndexCheck22') IS NOT NULL
  DROP TABLE tempdb.dbo.tmpIndexCheck22

/* Fabiano Amorim  */
/* http:\\www.blogfabiano.com | fabianonevesamorim@hotmail.com */
SET NOCOUNT ON; SET ANSI_WARNINGS ON;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
SET LOCK_TIMEOUT 50; /*if I get blocked for more than 50ms I'll quit, I don't want to wait or cause other blocks*/

/* Config params: */
DECLARE @TOP BIGINT = 10000 /* By default, I'm only reading TOP 10k plans */

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
FROM    sys.dm_exec_query_stats
OPTION (RECOMPILE);

IF OBJECT_ID('tempdb.dbo.#tmpdm_exec_query_stats_indx') IS NOT NULL
  DROP TABLE #tmpdm_exec_query_stats_indx

SELECT *
INTO #tmpdm_exec_query_stats_indx 
FROM sys.dm_exec_query_stats
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
      WHERE #tmpdm_exec_query_stats_indx.total_worker_time > 0 /* Only plans with CPU time > 0ms */
      AND NOT EXISTS(SELECT 1 
                     FROM sys.dm_exec_cached_plans
                     WHERE dm_exec_cached_plans.plan_handle = #tmpdm_exec_query_stats_indx.plan_handle
                     AND dm_exec_cached_plans.cacheobjtype = 'Compiled Plan Stub') /*Ignoring AdHoc - Plan Stub*/
      GROUP BY query_hash) AS t_dm_exec_query_stats
INNER JOIN sys.dm_exec_cached_plans
ON dm_exec_cached_plans.plan_handle = t_dm_exec_query_stats.plan_handle
ORDER BY query_impact DESC
OPTION (RECOMPILE);

ALTER TABLE #tmpdm_exec_query_stats ADD CONSTRAINT pk_tmpdm_exec_query_stats
PRIMARY KEY (plan_handle, statement_start_offset, statement_end_offset)

DECLARE @number_plans BIGINT,
        @err_msg      NVARCHAR(4000),
        @query_hash   VARBINARY(64),
        @plan_handle  VARBINARY(64),
        @statement_start_offset BIGINT, 
        @statement_end_offset BIGINT,
        @i            BIGINT

SELECT @number_plans = COUNT(*) 
FROM #tmpdm_exec_query_stats

SELECT @err_msg = '[' + CONVERT(NVARCHAR(200), GETDATE(), 120) + '] - ' + 'Starting to capture XML query plan for cached plans. Found ' + CONVERT(VARCHAR(200), @number_plans) + ' plans on sys.dm_exec_query_stats.'
RAISERROR (@err_msg, 0, 0) WITH NOWAIT

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
    SET @err_msg = '[' + CONVERT(VARCHAR(200), GETDATE(), 120) + '] - ' + 'Progress ' + '(' + CONVERT(VARCHAR(200), CONVERT(NUMERIC(25, 2), (CONVERT(NUMERIC(25, 2), @i) / CONVERT(NUMERIC(25, 2), @number_plans)) * 100)) + '%%) - ' 
                   + CONVERT(VARCHAR(200), @i) + ' of ' + CONVERT(VARCHAR(200), @number_plans)
    IF @i % 100 = 0
      RAISERROR (@err_msg, 0, 1) WITH NOWAIT

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
    OUTER APPLY (SELECT TRY_CONVERT(XML, detqp.query_plan)) AS t0 (query_plan)
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
								                                                       N'--' + NCHAR(13) + NCHAR(10) + t1.query + NCHAR(13) + NCHAR(10) + NCHAR(13) + NCHAR(10) + '/* Note: Query text was retrieved FROM showplan XML, and may be truncated. */' + NCHAR(13) + NCHAR(10) + N'--' COLLATE Latin1_General_Bin2
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
		END TRY
		BEGIN CATCH
			 --SELECT @err_msg = '[' + CONVERT(NVARCHAR(200), GETDATE(), 120) + '] - ' + 'Error trying to work on plan [' + CONVERT(NVARCHAR(800), @plan_handle, 1) + ']. Skipping this plan.'
    --RAISERROR (@err_msg, 0, 0) WITH NOWAIT
    --SELECT @err_msg = '[' + CONVERT(NVARCHAR(200), GETDATE(), 120) + '] - ' + ERROR_MESSAGE() 
    --RAISERROR (@err_msg, 0, 0) WITH NOWAIT
		END CATCH

  SET @i = @i + 1
  FETCH NEXT FROM c_plans
  INTO @query_hash, @plan_handle, @statement_start_offset, @statement_end_offset
END
CLOSE c_plans
DEALLOCATE c_plans

/* Remove rows that do not have missing index */
;WITH XMLNAMESPACES('http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p)
DELETE FROM #tmpdm_exec_query_stats
WHERE statement_plan.exist('.//p:MissingIndexes') = 0

SELECT @err_msg = '[' + CONVERT(NVARCHAR(200), GETDATE(), 120) + '] - ' + 'Finished to capture XML query plan for cached plans.'
RAISERROR (@err_msg, 0, 0) WITH NOWAIT

SELECT @err_msg = '[' + CONVERT(NVARCHAR(200), GETDATE(), 120) + '] - ' + 'Starting to collect data about last minute execution count.'
RAISERROR (@err_msg, 0, 0) WITH NOWAIT

/* Update execution_count_current with current number of executions */
UPDATE #tmpdm_exec_query_stats SET execution_count_current = dm_exec_query_stats.execution_count
FROM #tmpdm_exec_query_stats AS qs
INNER JOIN sys.dm_exec_query_stats
ON qs.plan_handle = dm_exec_query_stats.plan_handle
AND qs.statement_start_offset = dm_exec_query_stats.statement_start_offset
AND qs.statement_end_offset = dm_exec_query_stats.statement_end_offset

/* Wait for 1 minute */
WAITFOR DELAY '00:01:00.000'

/* Update execution_count_last_minute with number of executions on last minute */
UPDATE #tmpdm_exec_query_stats SET execution_count_last_minute = dm_exec_query_stats.execution_count - qs.execution_count_current
FROM #tmpdm_exec_query_stats AS qs
INNER JOIN sys.dm_exec_query_stats
ON qs.plan_handle = dm_exec_query_stats.plan_handle
AND qs.statement_start_offset = dm_exec_query_stats.statement_start_offset
AND qs.statement_end_offset = dm_exec_query_stats.statement_end_offset

SELECT @err_msg = '[' + CONVERT(NVARCHAR(200), GETDATE(), 120) + '] - ' + 'Finished to update data about last minute execution count.'
RAISERROR (@err_msg, 0, 0) WITH NOWAIT

SELECT @err_msg = '[' + CONVERT(NVARCHAR(200), GETDATE(), 120) + '] - ' + 'Starting to create XML indexes on #tmpdm_exec_query_stats.'
RAISERROR (@err_msg, 0, 0) WITH NOWAIT

CREATE PRIMARY XML INDEX ix1 ON #tmpdm_exec_query_stats(statement_plan)
CREATE XML INDEX ix2 ON #tmpdm_exec_query_stats(statement_plan)
USING XML INDEX ix1 FOR PROPERTY

SELECT @err_msg = '[' + CONVERT(NVARCHAR(200), GETDATE(), 120) + '] - ' + 'Finished to create XML indexes on #tmpdm_exec_query_stats.'
RAISERROR (@err_msg, 0, 0) WITH NOWAIT

SELECT @err_msg = '[' + CONVERT(NVARCHAR(200), GETDATE(), 120) + '] - ' + 'Starting to run final query'
RAISERROR (@err_msg, 0, 0) WITH NOWAIT

;WITH XMLNAMESPACES('http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p)
SELECT  'Check22 – Missing index plan cache' AS [Info],
        CASE database_id 
          WHEN 32767 THEN 'ResourceDB' 
          ELSE DB_NAME(database_id)
        END AS database_name,
        object_name,
        CONVERT(VARCHAR(800), query_hash, 1) AS query_hash,
        CONVERT(VARCHAR(800), plan_handle, 1) AS plan_handle,
        query_impact,

        Batch.x.value('count(//p:MissingIndexGroup)', 'int') AS missing_index_count, 
        index_impact,
        ix_db + '.' + ix_schema + '.' + ix_table AS table_name,
        key_cols,
        include_cols,
        t2.create_index_command,

        statement_text,
        statement_plan,
        execution_count,
        execution_count_percent_over_total,
        execution_count_per_minute,
        execution_count_current,
        execution_count_last_minute,
        /* 
           If there is only one execution, then, the compilation time can be calculated by
           checking the diff FROM the creation_time and last_execution_time.
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
        END AS compilation_time_FROM_dm_exec_query_stats,
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
INTO tempdb.dbo.tmpIndexCheck22
FROM #tmpdm_exec_query_stats qp
OUTER APPLY statement_plan.nodes('//p:Batch') AS Batch(x)
CROSS APPLY
  --Find the Missing Indexes Group Nodes in the Plan
  x.nodes('.//p:MissingIndexGroup') F_GrpNodes(GrpNode)
CROSS APPLY 
  --Pull out the Impact Figure
  (SELECT index_impact = GrpNode.value('(./@Impact)','float')) F_Impact
CROSS APPLY 
  --Get the Missing Index Nodes FROM the Group
  GrpNode.nodes('(./p:MissingIndex)') F_IxNodes(IxNode)
CROSS APPLY 
  --Pull out the Database,Schema,Table of the Missing Index
  (SELECT ix_db=IxNode.value('(./@Database)','sysname')
         ,ix_schema=IxNode.value('(./@Schema)','sysname')
         ,ix_table=IxNode.value('(./@Table)','sysname')
  ) F_IxInfo
CROSS APPLY 
  --Pull out the Key Columns and the Include Columns FROM the various Column Groups
  (SELECT eq_cols=MAX(CASE WHEN Usage='EQUALITY' THEN ColList END)
         ,ineq_cols=MAX(CASE WHEN Usage='INEQUALITY' THEN ColList END)
         ,include_cols=MAX(CASE WHEN Usage='INCLUDE' THEN ColList END)
   FROM IxNode.nodes('(./p:ColumnGroup)') F_ColGrp(ColGrpNode)
   CROSS APPLY 
     --Pull out the Usage of the Group? (EQUALITY of INEQUALITY or INCLUDE)
     (SELECT Usage=ColGrpNode.value('(./@Usage)','varchar(20)')) F_Usage
   CROSS APPLY 
     --Get a comma-delimited list of the Column Names in the Group
     (SELECT ColList=stuff((SELECT ','+ColNode.value('(./@Name)','sysname')
                            FROM ColGrpNode.nodes('(./p:Column)') F_ColNodes(ColNode)
                            FOR XML PATH(''))
                          ,1,1,'')
     ) F_ColList
  ) F_ColGrps
CROSS APPLY
  --Put together the Equality and InEquality Columns
  (SELECT key_cols=isnull(eq_cols,'')
                 +case 
                    when eq_cols is not null and ineq_cols is not null 
                    then ',' 
                    else '' 
                  end
                 +isnull(ineq_cols,'')
  ) F_KeyCols
CROSS APPLY 
  --Construct a CREATE INDEX command
  (SELECT create_index_command='USE ' + ix_db + ';' + NCHAR(13) + NCHAR(10) + 'GO' + NCHAR(13) + NCHAR(10) + NCHAR(13) + NCHAR(10)
                               + 'CREATE INDEX [<Name of Missing Index>] ON ' + ix_db + '.' + ix_schema + '.' + ix_table + ' (' + key_cols +')' 
                               + NCHAR(13) + NCHAR(10)
                               + ISNULL('INCLUDE (' + include_cols + ')' 
                               + NCHAR(13) + NCHAR(10),'')
                               + 'WITH(ONLINE = ON)' + NCHAR(13) + NCHAR(10) 
                               + 'GO' + NCHAR(13) + NCHAR(10)
                               ) F_Cmd 
OUTER APPLY (SELECT CONVERT(XML, ISNULL(CONVERT(XML, '<?index --' +
                                                        REPLACE
					                                                   (
						                                                   REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
						                                                   REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
						                                                   REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
							                                                   CONVERT
							                                                   (
								                                                   VARCHAR(MAX),
								                                                   N'--' + NCHAR(13) + NCHAR(10) + F_Cmd.create_index_command + N'--' COLLATE Latin1_General_Bin2
							                                                   ),
							                                                   NCHAR(31),N'?'),NCHAR(30),N'?'),NCHAR(29),N'?'),NCHAR(28),N'?'),NCHAR(27),N'?'),NCHAR(26),N'?'),NCHAR(25),N'?'),NCHAR(24),N'?'),NCHAR(23),N'?'),NCHAR(22),N'?'),
							                                                   NCHAR(21),N'?'),NCHAR(20),N'?'),NCHAR(19),N'?'),NCHAR(18),N'?'),NCHAR(17),N'?'),NCHAR(16),N'?'),NCHAR(15),N'?'),NCHAR(14),N'?'),NCHAR(12),N'?'),
							                                                   NCHAR(11),N'?'),NCHAR(8),N'?'),NCHAR(7),N'?'),NCHAR(6),N'?'),NCHAR(5),N'?'),NCHAR(4),N'?'),NCHAR(3),N'?'),NCHAR(2),N'?'),NCHAR(1),N'?'),
						                                                   NCHAR(0),
						                                                   N'')
                                                         + '--?>'),
                                              '<?index --' + NCHAR(13) + NCHAR(10) +
                                              'Statement not found.' + NCHAR(13) + NCHAR(10) +
                                              '--?>'))) AS t2 (create_index_command)
OPTION (RECOMPILE, MAXDOP 4);

SELECT @err_msg = '[' + CONVERT(NVARCHAR(200), GETDATE(), 120) + '] - ' + 'Finished to run final query'
RAISERROR (@err_msg, 0, 0) WITH NOWAIT

SELECT * FROM tempdb.dbo.tmpIndexCheck22
ORDER BY index_impact DESC