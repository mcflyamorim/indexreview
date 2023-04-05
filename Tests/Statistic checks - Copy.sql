USE [master]
GO

-- Collect statistics information and save it on tempdb.dbo.Tab_GetStatisticInfo_FabianoAmorim table
EXEC sp_GetStatisticInfo_FabianoAmorim 
  @DatabaseName = 'Northwind', /*Default is NULL, which means all user DBs*/
  @MinNumberOfRows = 10 /*Default is 100*/
GO

-- SELECT * FROM tempdb.dbo.Tab_GetStatisticInfo_FabianoAmorim a
-- WHERE TableName = '[QueryDefinitions]'

/*
  Starting Checks to understand the environment:
*/

SET NOCOUNT ON;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
SET LOCK_TIMEOUT 1000; /*1 second*/
SET DATEFORMAT mdy

/* 
  Check 1 - Do we have statistics with useful history? 
  By useful I mean statistics with information about at least more than 1 update. 
  If I have only 1 update, then, most of counters/checks won’t be available, 
  like, number of inserted rows since previous update and etc.

  This query will return all stats and number of statistics sample available,
  this also returns number of rows in the table, as if number of rows is small, I may (I'm not saying we don't, it depends) 
  don't care about this object.

  If statistic is updated recently, it maybe not a issue to have only 1 stat sample, as it may be a newly created stat.

  What to look for: 
  Ideally result for [Number of statistic data available for this object] should be 4, but 2 may be enough.
*/

SELECT 'Check 1 - Do we have statistics with useful history?' AS [Info],
       a.DatabaseName,
       a.TableName,
       a.StatsName,
       a.KeyColumnName,
       a.[Current number of rows on table],
       a.Statistic_Updated,
       (SELECT COUNT(b.[Number of statistic data]) FROM tempdb.dbo.Tab_GetStatisticInfo_FabianoAmorim b 
         WHERE b.DatabaseName = a.DatabaseName
           AND b.TableName   = a.TableName
           AND b.StatsName   = a.StatsName
       ) AS [Number of statistic data available for this object],
       CASE 
         WHEN (SELECT COUNT(b.[Number of statistic data]) FROM tempdb.dbo.Tab_GetStatisticInfo_FabianoAmorim b 
                 WHERE b.DatabaseName = a.DatabaseName
                   AND b.TableName   = a.TableName
                   AND b.StatsName   = a.StatsName
               ) = 1
         THEN 'Warning - There is only one statistic info for this obj. This will limit the  results of checks and may indicate update stats for this obj. is not running'
         ELSE 'OK'
       END AS [Comment 1]
  FROM tempdb.dbo.Tab_GetStatisticInfo_FabianoAmorim a
WHERE a.[Current number of rows on table] > 0 /* Ignoring empty tables */
GROUP BY a.DatabaseName,
         a.TableName,
         a.StatsName,
         a.KeyColumnName,
         a.[Current number of rows on table],
         a.Statistic_Updated 
ORDER BY a.[Current number of rows on table] DESC, 
         a.DatabaseName,
         a.TableName,
         a.KeyColumnName,
         a.StatsName
GO

/* 
  Check 2 - How often statistics is updated? 
  Statistics with only 1 sample indicate that statistic is not being updated.
  If more than one sample is found, this script will calculate what is the interval 
  average of time in minutes that statistic took to be updated.
*/

;WITH CTE_1
AS
(
SELECT a.DatabaseName,
       a.TableName,
       a.StatsName,
       a.KeyColumnName,
       a.[Current number of rows on table],
       a.Statistic_Updated AS [Update stat - 1 (most recent)],
       Tab_StatSample2.Statistic_Updated AS [Update stat - 2],
       Tab_StatSample3.Statistic_Updated AS [Update stat - 3],
       Tab_StatSample4.Statistic_Updated AS [Update stat - 4],
       a.[Number of modifications on key column since previous update] AS [Update stat - 1, Number of modifications on key column since previous update],
       Tab_StatSample2.[Number of modifications on key column since previous update] AS [Update stat - 2, Number of modifications on key column since previous update],
       Tab_StatSample3.[Number of modifications on key column since previous update] AS [Update stat - 3, Number of modifications on key column since previous update],
       DATEDIFF(MINUTE, Tab_StatSample2.Statistic_Updated, a.Statistic_Updated) AS [Minutes between update stats 1 and 2],
       DATEDIFF(MINUTE, Tab_StatSample3.Statistic_Updated, Tab_StatSample2.Statistic_Updated) AS [Minutes between update stats 2 and 3],
       DATEDIFF(MINUTE, Tab_StatSample4.Statistic_Updated, Tab_StatSample3.Statistic_Updated) AS [Minutes between update stats 3 and 4],
       (SELECT AVG(Col1) FROM (VALUES(DATEDIFF(MINUTE, Tab_StatSample2.Statistic_Updated, a.Statistic_Updated)), 
                                     (DATEDIFF(MINUTE, Tab_StatSample3.Statistic_Updated, Tab_StatSample2.Statistic_Updated)), 
                                     (DATEDIFF(MINUTE, Tab_StatSample4.Statistic_Updated, Tab_StatSample3.Statistic_Updated))
                               ) AS T(Col1)) AS [Avg minutes between update stats]
  FROM tempdb.dbo.Tab_GetStatisticInfo_FabianoAmorim a
 OUTER APPLY (SELECT b.[Number of rows on table at time statistic was updated],
                     b.[Number of modifications on key column since previous update],
                     b.[Statistic_Updated]
                FROM tempdb.dbo.Tab_GetStatisticInfo_FabianoAmorim b 
               WHERE b.DatabaseName = a.DatabaseName
                 AND b.TableName   = a.TableName
                 AND b.StatsName   = a.StatsName
                 AND b.[Number of statistic data] = 2 /* Previous update stat sample */
                ) AS Tab_StatSample2
 OUTER APPLY (SELECT b.[Number of rows on table at time statistic was updated],
                     b.[Number of modifications on key column since previous update],
                     b.[Statistic_Updated]
                FROM tempdb.dbo.Tab_GetStatisticInfo_FabianoAmorim b 
               WHERE b.DatabaseName = a.DatabaseName
                 AND b.TableName   = a.TableName
                 AND b.StatsName   = a.StatsName
                 AND b.[Number of statistic data] = 3 /* Previous update stat sample */
                ) AS Tab_StatSample3
 OUTER APPLY (SELECT b.[Number of rows on table at time statistic was updated],
                     b.[Number of modifications on key column since previous update],
                     b.[Statistic_Updated]
                FROM tempdb.dbo.Tab_GetStatisticInfo_FabianoAmorim b 
               WHERE b.DatabaseName = a.DatabaseName
                 AND b.TableName   = a.TableName
                 AND b.StatsName   = a.StatsName
                 AND b.[Number of statistic data] = 4 /* Previous update stat sample */
                ) AS Tab_StatSample4
WHERE a.[Current number of rows on table] > 0 /* Ignoring empty tables */
  AND a.[Number of statistic data] = 1
)
SELECT 'Check 2 - How often statistics is updated?' AS [Info],
       CTE_1.DatabaseName,
       CTE_1.TableName,
       CTE_1.StatsName,
       CTE_1.KeyColumnName,
       CTE_1.[Current number of rows on table],
       'Statistic is updated every ' 
       + CONVERT(VarChar(4), DATEDIFF(mi, '19000101', (DATEADD(mi, CTE_1.[Avg minutes between update stats], '19000101'))) / 60 / 24) + 'd '
       + CONVERT(VarChar(4), DATEDIFF(mi, '19000101', (DATEADD(mi, CTE_1.[Avg minutes between update stats], '19000101'))) / 60 % 24) + 'hr '
       + CONVERT(VarChar(4), DATEDIFF(mi, '19000101', (DATEADD(mi, CTE_1.[Avg minutes between update stats], '19000101'))) % 60) + 'min' AS [Comment 0],
       CTE_1.[Update stat - 1 (most recent)],
       CTE_1.[Update stat - 2],
       CTE_1.[Update stat - 3],
       CTE_1.[Update stat - 4],
       CTE_1.[Update stat - 1, Number of modifications on key column since previous update],
       CTE_1.[Update stat - 2, Number of modifications on key column since previous update],
       CTE_1.[Update stat - 3, Number of modifications on key column since previous update],
       CTE_1.[Minutes between update stats 1 and 2],
       CTE_1.[Minutes between update stats 2 and 3],
       CTE_1.[Minutes between update stats 3 and 4],
       CTE_1.[Avg minutes between update stats],
       CASE 
         WHEN ([Update stat - 1, Number of modifications on key column since previous update] = 0)
              OR ([Update stat - 2, Number of modifications on key column since previous update] = 0)
              OR ([Update stat - 3, Number of modifications on key column since previous update] = 0)
           THEN 'Warning - There was an event of statistic update with no modifications since last update. Make sure your maintenance script is smart enough to avoid update stats for non-modified stats.'
         ELSE 'OK'
       END AS [Comment 1],
       CASE 
         WHEN ([Minutes between update stats 1 and 2] <= 60)
              OR ([Minutes between update stats 2 and 3] = 60)
              OR ([Minutes between update stats 3 and 4] = 60)
           THEN 'Warning - There was an event of statistic update with interval of less than 1 hour. Those may be caused by a very high number of modifications triggering auto update or a bad job running unecessary updates.'
         ELSE 'OK'
       END AS [Comment 2],
       CASE 
         WHEN ([Minutes between update stats 1 and 2] >= 1500/*25 hours*/)
              OR ([Minutes between update stats 2 and 3] = 1500/*25 hours*/)
              OR ([Minutes between update stats 3 and 4] = 1500/*25 hours*/)
           THEN 'Warning - There was an event of statistic update with interval greater than 25 hours. If modification count is high, that will lead to poor exec plan estimations.'
         ELSE 'OK'
       END AS [Comment 3]
  FROM CTE_1
  WHERE 1=1
ORDER BY [Current number of rows on table] DESC, 
         DatabaseName,
         TableName,
         KeyColumnName,
         StatsName       
GO

-- Check 3 - Do I have plans with high compilation time due to a auto update/create stats?
-- https://techcommunity.microsoft.com/t5/azure-sql/diagnostic-data-for-synchronous-statistics-update-blocking/ba-p/386280

-- Check if statistic used in a query plan caused a long query plan compilation and optimization time
-- If last update timestamp of statistic is close to the query plan creation time, then, 
-- it is very likely that the update/create stat caused a higher query plan creation duration
-- How to do it? Query plan maybe very slow... Maybe grabing only plans with duration greater than 1 second.
-- If this is happening, recommend auto update stats asynchronous option.
/*
  With asynchronous statistics updates, queries compile with existing statistics even if the existing statistics are out-of-date. 
  The Query Optimizer could choose a suboptimal query plan if statistics are out-of-date when the query compiles. 
  Statistics are typically updated soon thereafter. 
  Queries that compile after the stats updates complete will benefit from using the updated statistics.
*/

IF OBJECT_ID('tempdb.dbo.#tmpdm_exec_query_stats_check3') IS NOT NULL
  DROP TABLE #tmpdm_exec_query_stats_check3
  
SELECT TOP 10000 
       qs.plan_handle,
       qs.statement_start_offset,
       qs.statement_end_offset,
       creation_time,
       last_execution_time,
       execution_count
INTO #tmpdm_exec_query_stats_check3
FROM sys.dm_exec_query_stats qs
WHERE 1=1
AND DATEDIFF(hour, creation_time, GETDATE()) <= 48 /*Only plans created in past 48 hours*/
--AND qs.max_elapsed_time > 500000 /*Only plans with max_elapsed_time greater than 500ms*/
ORDER BY execution_count DESC

CREATE CLUSTERED INDEX ix1 ON #tmpdm_exec_query_stats_check3(plan_handle)
DBCC TRACEON(8666)
IF OBJECT_ID('tempdb.dbo.#query_plan') IS NOT NULL
  DROP TABLE #query_plan

SELECT qs.plan_handle, 
       TRY_CONVERT(XML, detqp.query_plan) AS StatementPlan, 
       TRY_CONVERT(XML, Tab2.Col1) AS StatementText,
       creation_time,
       last_execution_time,
       execution_count
INTO #query_plan
FROM #tmpdm_exec_query_stats_check3 qs
OUTER APPLY sys.dm_exec_sql_text(qs.plan_handle) st
OUTER APPLY (SELECT CHAR(13)+CHAR(10) + st.text + CHAR(13)+CHAR(10) FOR XML RAW, ELEMENTS) AS Tab1(Col1)
OUTER APPLY (SELECT -- Extract statement from sql text
                    CHAR(13)+CHAR(10) + 
                    ISNULL(
                        NULLIF(
                            SUBSTRING(
                              st.text, 
                              qs.statement_start_offset / 2, 
                              CASE WHEN qs.statement_end_offset < qs.statement_start_offset 
                               THEN 0
                              ELSE( qs.statement_end_offset - qs.statement_start_offset ) / 2 END + 2
                            ), ''
                        ), st.text
                    ) + CHAR(13)+CHAR(10) + CHAR(13)+CHAR(10) FOR XML RAW, ELEMENTS) AS Tab2(Col1)
OUTER APPLY sys.dm_exec_text_query_plan(qs.plan_handle,
                                          qs.statement_start_offset,
                                          qs.statement_end_offset) AS detqp
OUTER APPLY (SELECT TRY_CONVERT(XML, detqp.query_plan) AS qXML) AS TabPlanXML
WHERE 1=1
AND TRY_CONVERT(XML, detqp.query_plan) IS NOT NULL

DBCC TRACEOFF(8666)

IF NOT EXISTS(SELECT * FROM tempdb.dbo.sysindexes where name = 'ixNumStats_StatsDate')
BEGIN
  CREATE INDEX ixNumStats_StatsDate 
  ON tempdb.dbo.Tab_GetStatisticInfo_FabianoAmorim([Number of statistic data], [Statistic_Updated]) 
  INCLUDE(StatsName, DatabaseName, TableName, [Current number of rows on table])
END

IF OBJECT_ID('tempdb.dbo.#tmp1') IS NOT NULL
  DROP TABLE #tmp1

;WITH XMLNAMESPACES('http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p)
SELECT  Plan_handle, 
        StatementText,
        StatementPlan,
        last_execution_time,
        execution_count,
        StatementType = COALESCE(Batch.x.value('(//p:StmtSimple/@StatementType)[1]', 'VarChar(500)'),
                                 Batch.x.value('(//p:StmtCond/@StatementType)[1]', 'VarChar(500)'),
                                 Batch.x.value('(//p:StmtCursor/@StatementType)[1]', 'VarChar(500)'),
                                 Batch.x.value('(//p:StmtReceive/@StatementType)[1]', 'VarChar(500)'),
                                 Batch.x.value('(//p:StmtUseDb/@StatementType)[1]', 'VarChar(500)')),
        CachedPlanSize = x.value('sum(..//p:QueryPlan/@CachedPlanSize)', 'float'),
        CompileTime = x.value('sum(..//p:QueryPlan/@CompileTime)', 'float'),
        CASE 
         WHEN execution_count = 1
         THEN DATEDIFF(ms, creation_time, last_execution_time)
         ELSE NULL
        END AS CompilationTimeFrom_dm_exec_query_stats,
        CompileCPU = x.value('sum(..//p:QueryPlan/@CompileCPU)', 'float'),
        CompileMemory = x.value('sum(..//p:QueryPlan/@CompileMemory)', 'float'),
        creation_time AS [ExecPlan creation start time],
        [Associated stats update time] = (SELECT TOP 1 CONVERT(VarChar, a.[Statistic_Updated], 21)
                                          FROM tempdb.dbo.Tab_GetStatisticInfo_FabianoAmorim AS a
                                          WHERE a.[Number of statistic data] = 1
                                          AND a.[Statistic_Updated] >= creation_time
                                          ORDER BY a.[Statistic_Updated] ASC),
        DATEADD(ms, x.value('sum(..//p:QueryPlan/@CompileTime)', 'float'), creation_time) AS [ExecPlan creation end time],
        [Associated stats name] = (SELECT TOP 1 a.StatsName
                                   FROM tempdb.dbo.Tab_GetStatisticInfo_FabianoAmorim AS a
                                   WHERE a.[Number of statistic data] = 1
                                   AND a.[Statistic_Updated] >= creation_time
                                   ORDER BY a.[Statistic_Updated] ASC),
        [Statistic associated with compile] = (SELECT TOP 1
                                                      'Statistic ' + a.StatsName + 
                                                      ' on table ' + a.DatabaseName + '.' + a.TableName + ' ('+ CONVERT(VarChar, a.[Current number of rows on table]) +' rows)' +
                                                      ' was updated about the same time (' + CONVERT(VarChar, a.[Statistic_Updated], 21) + ') that the plan was created, that may be the reason of the high compile time.'
                                                FROM tempdb.dbo.Tab_GetStatisticInfo_FabianoAmorim AS a
                                                WHERE a.[Number of statistic data] = 1
                                                AND a.[Statistic_Updated] >= creation_time
                                                ORDER BY a.[Statistic_Updated] ASC)
INTO #tmp1
FROM #query_plan qp
OUTER APPLY StatementPlan.nodes('//p:Batch') AS Batch(x)
--WHERE x.value('sum(..//p:QueryPlan/@CompileTime)', 'float') > 500
OPTION (RECOMPILE);

SELECT 'Check 3 - Do I have plans with high compilation time due to a auto update/create stats?' AS [Info], 
       * 
FROM #tmp1
WHERE 1=1
AND [Associated stats update time] <= [ExecPlan creation end time]
AND CONVERT(VarChar(MAX), StatementPlan) COLLATE Latin1_General_BIN2 LIKE '%' + REPLACE(REPLACE([Associated stats name], '[', ''), ']', '') + '%'
OR [Associated stats name] IS NULL
ORDER BY CompileTime DESC


-- Check 4 - 
-- Is there a sort Warning on default trace at the same time last update stats happened? 
---- That may tell us that the StatMan query is spilling data to disk

IF OBJECT_ID('tempdb.dbo.#tmpCheckSortWarning') IS NOT NULL
  DROP TABLE #tmpCheckSortWarning

-- Declare variables
DECLARE @filename NVarChar(1000);
DECLARE @bc INT;
DECLARE @ec INT;
DECLARE @bfn VarChar(1000);
DECLARE @efn VarChar(10);

-- Get the name of the current default trace
SELECT @filename = CAST(value AS NVarChar(1000))
FROM::fn_trace_getinfo(DEFAULT)
WHERE traceid = 1
      AND property = 2;

-- rip apart file name into pieces
SET @filename = REVERSE(@filename);
SET @bc = CHARINDEX('.', @filename);
SET @ec = CHARINDEX('_', @filename) + 1;
SET @efn = REVERSE(SUBSTRING(@filename, 1, @bc));
SET @bfn = REVERSE(SUBSTRING(@filename, @ec, LEN(@filename)));

-- set filename without rollover number
SET @filename = @bfn + @efn;

-- process all trace files
SELECT ftg.spid,
       te.name,
       ftg.EventSubClass,
       ftg.StartTime,
       ftg.ApplicationName,
       ftg.Hostname,
       DB_NAME(ftg.databaseID) AS DBName,
       ftg.LoginName
INTO #tmpCheckSortWarning
FROM::fn_trace_gettable(@filename, DEFAULT) AS ftg
    INNER JOIN sys.trace_events AS te
        ON ftg.EventClass = te.trace_event_id
WHERE te.name = 'Sort Warnings'
ORDER BY ftg.StartTime ASC, ftg.spid;

CREATE CLUSTERED INDEX ix1 ON #tmpCheckSortWarning(StartTime)

SELECT 'Check 4 - Is there a sort Warning on default trace at the same time last update stats happened?' AS [Info],
       a.DatabaseName,
       a.TableName,
       a.StatsName,
       a.KeyColumnName,
       a.[Number of rows on table at time statistic was updated],
       a.Statistic_Updated,
       Tab1.ClosestSortWarning,
       DATEDIFF(MILLISECOND, Tab1.ClosestSortWarning, a.Statistic_Updated) AS [Diff of update stats to the sort Warning in ms],
       CASE 
         WHEN DATEDIFF(MILLISECOND, Tab1.ClosestSortWarning, a.Statistic_Updated) BETWEEN 0 AND 10 THEN 'Sort Warning was VERY CLOSE (less than 10ms diff) to the update stats, very high chances this was triggered by the update stats'
         WHEN DATEDIFF(MILLISECOND, Tab1.ClosestSortWarning, a.Statistic_Updated) BETWEEN 11 AND 50 THEN 'Sort Warning was CLOSE (between than 11 and 50ms diff) to the update stats, still very high chances this was triggered by the update stats'
         WHEN DATEDIFF(MILLISECOND, Tab1.ClosestSortWarning, a.Statistic_Updated) BETWEEN 51 AND 100 THEN 'Sort Warning was CLOSE (between than 51 and 100ms diff) to the update stats, high chances this was triggered by the update stats'
         WHEN DATEDIFF(MILLISECOND, Tab1.ClosestSortWarning, a.Statistic_Updated) BETWEEN 101 AND 500 THEN 'Sort Warning was NEAR (between than 101 and 500ms diff) to the update stats, high chances this was triggered by the update stats'
         WHEN a.[Number of rows on table at time statistic was updated] >= 1000000 AND DATEDIFF(MILLISECOND, Tab1.ClosestSortWarning, a.Statistic_Updated) BETWEEN 501 AND 20000 THEN 'Sort Warning was not close (between than 501 and 20000ms diff) to the update stats, but, since number of rows on table is greater than 1mi, depending on how much time spill took, update stat may still be related to this Warning'
         ELSE 'Very unnlikely this was related to the update stats, but, may be.'
       END [Comment 1]
FROM tempdb.dbo.Tab_GetStatisticInfo_FabianoAmorim a
CROSS APPLY (SELECT TOP 1 StartTime FROM #tmpCheckSortWarning
              WHERE #tmpCheckSortWarning.StartTime <= a.Statistic_Updated
              ORDER BY StartTime DESC) AS Tab1(ClosestSortWarning)
WHERE (a.[Number of rows on table at time statistic was updated] >= 10000 or a.IsLOB = 1) /* Ignoring small tables unless is LOB*/
  AND a.[Number of statistic data] = 1
ORDER BY a.Statistic_Updated DESC
GO


-- Check 5 - Is there any unused statistics?
/*
  Check unused statistics
  If number of modifications is greather than the auto update threshold, 
  then statistic may not be used (considering auto update stats is on on DB)
  If the number of changes is higher than the threshold but the statistic is not updated, 
  that means this statistic is not used since last update time (or Auto Update Statistics option is set to OFF).
  Check how many days has been since last update stats and current date to see for how long this stat
  is considered as "not used"

  Hypothetical indexes will show up as unused as they usually do not get updated by maintenance plans.
  If you see %_dta_% garbage on this, please drop those indexes and stats.
*/

IF OBJECT_ID('tempdb.dbo.#tmpCheck5') IS NOT NULL
  DROP TABLE #tmpCheck5

SELECT a.DatabaseName,
       CASE a.is_auto_create_stats_on WHEN 1 THEN 'Yes' ELSE 'No' END AS [Is DB AutoUpdateStatsOn],
       a.TableName,
       a.StatsName,
       a.KeyColumnName,
       a.[Statistic type],
       a.[Statistic_Updated],
       CONVERT(VarChar(4), DATEDIFF(mi,a.[Statistic_Updated],GETDATE()) / 60 / 24) + 'd ' + CONVERT(VarChar(4), DATEDIFF(mi,a.[Statistic_Updated],GETDATE()) / 60 % 24) + 'hr '
       + CONVERT(VarChar(4), DATEDIFF(mi,a.[Statistic_Updated],GETDATE()) % 60) + 'min' AS [Time since last update],
       TabIndexUsage.[Last time index(or a table if obj is not a Index_Statistic) was used],
       CONVERT(VarChar(4), DATEDIFF(mi,TabIndexUsage.[Last time index(or a table if obj is not a Index_Statistic) was used],GETDATE()) / 60 / 24) + 'd ' + CONVERT(VarChar(4), DATEDIFF(mi,TabIndexUsage.[Last time index(or a table if obj is not a Index_Statistic) was used],GETDATE()) / 60 % 24) + 'hr '
       + CONVERT(VarChar(4), DATEDIFF(mi,TabIndexUsage.[Last time index(or a table if obj is not a Index_Statistic) was used],GETDATE()) % 60) + 'min' AS [Time since last index(or a table if obj is not a Index_Statistic) usage],

       a.[Current number of rows on table], 
       a.[Number of rows on table at time statistic was updated],
       a.[Current number of modified rows since last update],
       Tab3.AutoUpdateThreshold,
       Tab3.AutoUpdateThresholdType,
       CONVERT(DECIMAL(18, 2), (a.[Current number of modified rows since last update] / (Tab3.AutoUpdateThreshold * 1.0)) * 100.0) AS [Percent of threshold]
INTO #tmpCheck5
FROM tempdb.dbo.Tab_GetStatisticInfo_FabianoAmorim a
LEFT OUTER JOIN sys.dm_db_index_usage_stats
ON a.DatabaseID = dm_db_index_usage_stats.database_id
AND a.ObjectID = dm_db_index_usage_stats.object_id
AND (dm_db_index_usage_stats.index_id = CASE WHEN a.[statistic type] = 'Index_Statistic' THEN a.stats_id ELSE 1 END)
OUTER APPLY (SELECT MIN(Dt) FROM (VALUES(dm_db_index_usage_stats.last_user_seek), 
                                        (dm_db_index_usage_stats.last_user_scan)
                               ) AS t(Dt)) AS TabIndexUsage([Last time index(or a table if obj is not a Index_Statistic) was used])
CROSS APPLY (SELECT CASE 
                       WHEN (SELECT compatibility_level 
                               FROM sys.databases 
                              WHERE QUOTENAME(name) = a.DatabaseName) >= 130 
                            AND 
                            COALESCE(a.unfiltered_rows, 0) >= 25001
                         THEN 'Dynamic'
                       ELSE 'Static'
                     END, 
                     CASE 
                       WHEN (SELECT compatibility_level 
                               FROM sys.databases 
                              WHERE QUOTENAME(name) = a.DatabaseName) >= 130
                            AND 
                            COALESCE(a.unfiltered_rows, 0) >= 25001
                         THEN CONVERT(BIGINT, SQRT(1000 * COALESCE(a.unfiltered_rows, 0)))
                       ELSE (CASE
				                           WHEN COALESCE(a.unfiltered_rows, 0) IS NULL THEN 0
				                           WHEN COALESCE(a.unfiltered_rows, 0) <= 500 THEN 501
				                           ELSE 500 + CONVERT(BIGINT, COALESCE(a.unfiltered_rows, 0) * 0.2)
			                          END)
                     END) AS Tab3(AutoUpdateThresholdType, AutoUpdateThreshold)
WHERE a.[Number of statistic data] = 1
AND CONVERT(DECIMAL(18, 2), (a.[Current number of modified rows since last update] / (Tab3.AutoUpdateThreshold * 1.0)) * 100.0) >= 100 /*Only rows with threshold already hit*/
AND a.is_auto_create_stats_on = 1 /*Considering only DBs with auto update stats on*/
AND a.no_recompute = 0 /*Considering only stats with no recompute off*/
AND a.stats_id <> 1 /*Ignoring clustered keys has they can still be used in lookups and don't trigger update stats*/
AND DATEDIFF(HOUR, a.[Statistic_Updated], GETDATE()) >= 48 /*Only considering statistics that were not updated in past 2 days*/

SELECT 'Check 5 - Is there any unused statistics?' AS [Info], 
       * 
FROM #tmpCheck5
ORDER BY [Current number of rows on table] DESC, 
         DatabaseName,
         TableName,
         KeyColumnName,
         StatsName
GO

-- Check 6 - How many modifications per minute we've? 

SELECT 'Check 6 - How many modifications per minute we have?' AS [Info],
       a.DatabaseName,
       a.TableName,
       a.StatsName,
       a.KeyColumnName,
       a.[Statistic_Updated],
       a.[Current number of rows on table], 
       a.[Number of rows on table at time statistic was updated],
       a.unfiltered_rows AS [Number of rows on table at time statistics was updated ignoring filter],
       a.[Current number of modified rows since last update],
       TabModificationsPerMinute.[Avg modifications per minute based on existing update stats intervals],
       CONVERT(NUMERIC(18, 2), a.[Current number of modified rows since last update] 
       / CASE DATEDIFF(minute, a.Statistic_Updated, GETDATE()) WHEN 0 THEN 1 ELSE DATEDIFF(minute, a.Statistic_Updated, GETDATE()) END) AS [Avg modifications per minute based on current GetDate],
      user_seeks + user_scans + user_lookups AS [Number of reads on index/table since last restart],
      user_seeks + user_scans + user_lookups / 
      CASE DATEDIFF(mi, (SELECT create_date FROM sys.databases WHERE name = 'tempdb'), getdate())
        WHEN 0 THEN 1
        ELSE DATEDIFF(mi, (SELECT create_date FROM sys.databases WHERE name = 'tempdb'), getdate())
      END AS [Avg of reads per minute based on index usage dmv],
      user_updates AS [Number of modifications on index/table since last restart],
      user_updates /
      CASE DATEDIFF(mi, (SELECT create_date FROM sys.databases WHERE name = 'tempdb'), getdate())
        WHEN 0 THEN 1
        ELSE DATEDIFF(mi, (SELECT create_date FROM sys.databases WHERE name = 'tempdb'), getdate())
      END AS [Avg of modifications per minute based on index usage dmv],
      range_scan_count AS [Number of range scans since last restart/rebuild],
      page_latch_wait_count AS [Number of page latch since last restart/rebuild],
      page_io_latch_wait_count AS [Number of page I/O latch since last restart/rebuild]
  FROM tempdb.dbo.Tab_GetStatisticInfo_FabianoAmorim AS a
 OUTER APPLY (SELECT b.[Number of rows on table at time statistic was updated],
                     b.[Number of modifications on key column since previous update],
                     b.[Statistic_Updated]
                FROM tempdb.dbo.Tab_GetStatisticInfo_FabianoAmorim b 
               WHERE b.DatabaseName = a.DatabaseName
                 AND b.TableName   = a.TableName
                 AND b.StatsName   = a.StatsName
                 AND b.[Number of statistic data] = 2 /* Previous update stat sample */
                ) AS Tab_StatSample2
 OUTER APPLY (SELECT b.[Number of rows on table at time statistic was updated],
                     b.[Number of modifications on key column since previous update],
                     b.[Statistic_Updated]
                FROM tempdb.dbo.Tab_GetStatisticInfo_FabianoAmorim b 
               WHERE b.DatabaseName = a.DatabaseName
                 AND b.TableName   = a.TableName
                 AND b.StatsName   = a.StatsName
                 AND b.[Number of statistic data] = 3 /* Previous update stat sample */
                ) AS Tab_StatSample3
 OUTER APPLY (SELECT b.[Number of rows on table at time statistic was updated],
                     b.[Number of modifications on key column since previous update],
                     b.[Statistic_Updated]
                FROM tempdb.dbo.Tab_GetStatisticInfo_FabianoAmorim b 
               WHERE b.DatabaseName = a.DatabaseName
                 AND b.TableName   = a.TableName
                 AND b.StatsName   = a.StatsName
                 AND b.[Number of statistic data] = 4 /* Previous update stat sample */
                ) AS Tab_StatSample4
 CROSS APPLY (SELECT SUM(Col1) FROM (VALUES(DATEDIFF(MINUTE, Tab_StatSample2.Statistic_Updated, a.Statistic_Updated)), 
                                           (DATEDIFF(MINUTE, Tab_StatSample3.Statistic_Updated, Tab_StatSample2.Statistic_Updated)), 
                                           (DATEDIFF(MINUTE, Tab_StatSample4.Statistic_Updated, Tab_StatSample3.Statistic_Updated))
                                ) AS Tab(Col1)) AS Tab_MinBetUpdateStats([Tot minutes between update stats])
 CROSS APPLY (SELECT SUM(Col1) FROM (VALUES(a.[Number of modifications on key column since previous update]), 
                                           (Tab_StatSample2.[Number of modifications on key column since previous update]), 
                                           (Tab_StatSample3.[Number of modifications on key column since previous update])
                                ) AS Tab(Col1)) AS Tab_TotModifications([Tot modifications between update stats])
 CROSS APPLY (SELECT CONVERT(NUMERIC(18, 2), Tab_TotModifications.[Tot modifications between update stats] 
                     / CASE 
                         WHEN Tab_MinBetUpdateStats.[Tot minutes between update stats] = 0 THEN 1 
                         ELSE Tab_MinBetUpdateStats.[Tot minutes between update stats] 
                       END)) AS TabModificationsPerMinute([Avg modifications per minute based on existing update stats intervals])
 WHERE a.[Number of statistic data] = 1
   AND a.[Current number of rows on table] > 100 /* Ignoring "small" tables */
   AND a.stats_id = 1 /*Considering only clustered index to show data per table, yep, no info for heaps*/
ORDER BY [Current number of rows on table] DESC, 
         DatabaseName,
         TableName,
         KeyColumnName,
         StatsName
GO

-- Check 7 - What is the updatestat threshold for each statistic?
SELECT 'Check 7 - What is the updatestat threshold for each statistic?' AS [Info],
       a.DatabaseName,
       a.TableName,
       a.StatsName,
       a.KeyColumnName,
       a.[Statistic_Updated],
       TabIndexUsage.[Last time index(or a table if obj is not a Index_Statistic) was used],
       a.[Current number of rows on table], 
       a.[Number of rows on table at time statistic was updated],
       a.unfiltered_rows AS [Number of rows on table at time statistics was updated ignoring filter],
       a.[Current number of modified rows since last update],
       Tab3.AutoUpdateThreshold,
       Tab3.AutoUpdateThresholdType,
	      CONVERT(DECIMAL(18, 2), (a.[Current number of modified rows since last update] / (Tab3.AutoUpdateThreshold * 1.0)) * 100.0) AS [Percent of threshold],
       CASE 
         WHEN TabModificationsPerMinute.[Avg modifications per minute based on existing update stats intervals] > 0 THEN
				     DATEADD(MINUTE, ((Tab3.AutoUpdateThreshold - a.[Current number of modified rows since last update]) / TabModificationsPerMinute.[Avg modifications per minute based on existing update stats intervals]), GETDATE())
			      ELSE NULL
		     END AS [Estimated date of next auto update stats],
       TabEstimatedMinsUntilNextUpdateStats.[Estimated minutes until next auto update stats],
       CASE 
          WHEN a.is_auto_update_stats_on = 1
                AND [Estimated minutes until next auto update stats] <= 0
          THEN 'Warning - Auto update stats will be executed on next execution of query using this statistic'
          WHEN a.is_auto_update_stats_on = 1
                AND [Estimated minutes until next auto update stats] <= 120
          THEN 'Warning - Auto update stats will be executed in about 2 hours on next execution of query using this statistic'
          WHEN a.is_auto_update_stats_on = 0
                AND [Estimated minutes until next auto update stats] <= 0
          THEN 'Warning - AutoUpdateStats on DB is OFF, but statistic already hit the threshold to trigger auto update stats. Queries using this statistic are likely to be using outdated stats.' 
          ELSE 'OK'
       END AS [Comment 1],
       a.[Number of modifications on key column since previous update] AS [Update stat - 1, Number of modifications on key column since previous update],
       Tab_StatSample2.[Number of modifications on key column since previous update] AS [Update stat - 2, Number of modifications on key column since previous update],
       Tab_StatSample3.[Number of modifications on key column since previous update] AS [Update stat - 3, Number of modifications on key column since previous update],
       Tab_MinBetUpdateStats.[Tot minutes between update stats],
       Tab_TotModifications.[Tot modifications between update stats],
       TabModificationsPerMinute.[Avg modifications per minute based on existing update stats intervals],
       TabModificationsPerMinute2.[Avg modifications per minute based on current GetDate]
  FROM tempdb.dbo.Tab_GetStatisticInfo_FabianoAmorim AS a
 CROSS APPLY (SELECT CASE 
                       WHEN (SELECT compatibility_level 
                               FROM sys.databases 
                              WHERE QUOTENAME(name) = a.DatabaseName) >= 130 
                            AND 
                            COALESCE(a.unfiltered_rows, 0) >= 25001
                         THEN 'Dynamic'
                       ELSE 'Static'
                     END, 
                     CASE 
                       WHEN (SELECT compatibility_level 
                               FROM sys.databases 
                              WHERE QUOTENAME(name) = a.DatabaseName) >= 130
                            AND 
                            COALESCE(a.unfiltered_rows, 0) >= 25001
                         THEN CONVERT(BIGINT, SQRT(1000 * COALESCE(a.unfiltered_rows, 0)))
                       ELSE (CASE
				                           WHEN COALESCE(a.unfiltered_rows, 0) IS NULL THEN 0
				                           WHEN COALESCE(a.unfiltered_rows, 0) <= 500 THEN 501
				                           ELSE 500 + CONVERT(BIGINT, COALESCE(a.unfiltered_rows, 0) * 0.2)
			                          END)
                     END) AS Tab3(AutoUpdateThresholdType, AutoUpdateThreshold)
 OUTER APPLY (SELECT b.[Number of rows on table at time statistic was updated],
                     b.[Number of modifications on key column since previous update],
                     b.[Statistic_Updated]
                FROM tempdb.dbo.Tab_GetStatisticInfo_FabianoAmorim b 
               WHERE b.DatabaseName = a.DatabaseName
                 AND b.TableName   = a.TableName
                 AND b.StatsName   = a.StatsName
                 AND b.[Number of statistic data] = 2 /* Previous update stat sample */
                ) AS Tab_StatSample2
 OUTER APPLY (SELECT b.[Number of rows on table at time statistic was updated],
                     b.[Number of modifications on key column since previous update],
                     b.[Statistic_Updated]
                FROM tempdb.dbo.Tab_GetStatisticInfo_FabianoAmorim b 
               WHERE b.DatabaseName = a.DatabaseName
                 AND b.TableName   = a.TableName
                 AND b.StatsName   = a.StatsName
                 AND b.[Number of statistic data] = 3 /* Previous update stat sample */
                ) AS Tab_StatSample3
 OUTER APPLY (SELECT b.[Number of rows on table at time statistic was updated],
                     b.[Number of modifications on key column since previous update],
                     b.[Statistic_Updated]
                FROM tempdb.dbo.Tab_GetStatisticInfo_FabianoAmorim b 
               WHERE b.DatabaseName = a.DatabaseName
                 AND b.TableName   = a.TableName
                 AND b.StatsName   = a.StatsName
                 AND b.[Number of statistic data] = 4 /* Previous update stat sample */
                ) AS Tab_StatSample4
 CROSS APPLY (SELECT SUM(Col1) FROM (VALUES(DATEDIFF(MINUTE, Tab_StatSample2.Statistic_Updated, a.Statistic_Updated)), 
                                           (DATEDIFF(MINUTE, Tab_StatSample3.Statistic_Updated, Tab_StatSample2.Statistic_Updated)), 
                                           (DATEDIFF(MINUTE, Tab_StatSample4.Statistic_Updated, Tab_StatSample3.Statistic_Updated))
                                ) AS Tab(Col1)) AS Tab_MinBetUpdateStats([Tot minutes between update stats])
 CROSS APPLY (SELECT SUM(Col1) FROM (VALUES(a.[Number of modifications on key column since previous update]), 
                                           (Tab_StatSample2.[Number of modifications on key column since previous update]), 
                                           (Tab_StatSample3.[Number of modifications on key column since previous update])
                                ) AS Tab(Col1)) AS Tab_TotModifications([Tot modifications between update stats])
 CROSS APPLY (SELECT CONVERT(NUMERIC(18, 2), Tab_TotModifications.[Tot modifications between update stats] 
                     / CASE 
                         WHEN Tab_MinBetUpdateStats.[Tot minutes between update stats] = 0 THEN 1 
                         ELSE Tab_MinBetUpdateStats.[Tot minutes between update stats] 
                       END)) AS TabModificationsPerMinute([Avg modifications per minute based on existing update stats intervals])
 CROSS APPLY (SELECT CONVERT(NUMERIC(18, 2), a.[Current number of modified rows since last update] 
                     / 
                     CASE DATEDIFF(minute, a.Statistic_Updated, GETDATE()) WHEN 0 THEN 1 ELSE DATEDIFF(minute, a.Statistic_Updated, GETDATE()) END)) AS TabModificationsPerMinute2([Avg modifications per minute based on current GetDate]) 
 CROSS APPLY (SELECT DATEDIFF(MINUTE, GETDATE(), CASE 
                                                   WHEN ISNULL(TabModificationsPerMinute.[Avg modifications per minute based on existing update stats intervals], TabModificationsPerMinute2.[Avg modifications per minute based on current GetDate])  > 0 THEN
				                                               DATEADD(MINUTE, ((Tab3.AutoUpdateThreshold - a.[Current number of modified rows since last update]) / ISNULL(TabModificationsPerMinute.[Avg modifications per minute based on existing update stats intervals], TabModificationsPerMinute2.[Avg modifications per minute based on current GetDate])), GETDATE())
			                                                ELSE NULL
		                                               END)) AS TabEstimatedMinsUntilNextUpdateStats([Estimated minutes until next auto update stats])
LEFT OUTER JOIN sys.dm_db_index_usage_stats
ON a.DatabaseID = dm_db_index_usage_stats.database_id
AND a.ObjectID = dm_db_index_usage_stats.object_id
AND (dm_db_index_usage_stats.index_id = CASE WHEN a.[statistic type] = 'Index_Statistic' THEN a.stats_id ELSE 1 END)
OUTER APPLY (SELECT MIN(Dt) FROM (VALUES(dm_db_index_usage_stats.last_user_seek), 
                                        (dm_db_index_usage_stats.last_user_scan), 
                                        (dm_db_index_usage_stats.last_user_lookup)
                               ) AS t(Dt)) AS TabIndexUsage([Last time index(or a table if obj is not a Index_Statistic) was used])
 WHERE a.[Number of statistic data] = 1
   AND a.[Current number of rows on table] > 0 /* Ignoring empty tables */
ORDER BY [Current number of rows on table] DESC, 
         DatabaseName,
         TableName,
         KeyColumnName,
         StatsName


-- Check 8 - Is there any tiny table (less than or equal to 500 rows) with outdated statistics?
/*
  Check if there are small tables (less than or equal to 500 rows) with poor statistics
  those small tables will only trigger auto-update stats if modification counter is 
  >= 501, depending on the environment this may take a while or never happen.
  To avoid outdated or obsolete statistics on those tiny tables (in terms of number of rows), 
  make sure you're manually updating it... it won't take too much time and may help query optimizer.

  You can also use column [Query plan associated with last usage] to investigate query plan.
*/

IF OBJECT_ID('tempdb.dbo.#tmpdm_exec_query_stats_check8') IS NOT NULL
  DROP TABLE #tmpdm_exec_query_stats_check8
  
SELECT qs.plan_handle,
       qs.statement_start_offset,
       qs.statement_end_offset,
       last_execution_time
INTO #tmpdm_exec_query_stats_check8
FROM sys.dm_exec_query_stats qs
CREATE NONCLUSTERED INDEX ix1 ON #tmpdm_exec_query_stats_check8(last_execution_time)

SELECT 'Check 8 - Is there any tiny table (less than or equal to 500 rows) with outdated statistics?' AS [Info],
       a.DatabaseName,
       a.TableName,
       a.StatsName,
       a.[statistic type],
       a.KeyColumnName,
       a.[Statistic_Updated], 
       a.[Statistic updated comment],
       TabIndexUsage.[Last time index(or a table if obj is not a Index_Statistic) was used],
       CASE 
         WHEN a.[statistic type] = 'Index_Statistic' 
          AND DATEDIFF(hour, TabIndexUsage.[Last time index(or a table if obj is not a Index_Statistic) was used], GETDATE()) <= 6 /*If last usage was in past 6 hours, try to see if can find associated query plan from cache plan*/
         THEN (
               SELECT TOP 1 TRY_CONVERT(XML, detqp.query_plan) AS StatementPlan
               FROM #tmpdm_exec_query_stats_check8 qs
               OUTER APPLY sys.dm_exec_sql_text(qs.plan_handle) st
               OUTER APPLY (SELECT CHAR(13)+CHAR(10) + st.text + CHAR(13)+CHAR(10) FOR XML RAW, ELEMENTS) AS Tab1(Col1)
               OUTER APPLY (SELECT -- Extract statement from sql text
                                   CHAR(13)+CHAR(10) + 
                                   ISNULL(
                                       NULLIF(
                                           SUBSTRING(
                                             st.text, 
                                             qs.statement_start_offset / 2, 
                                             CASE WHEN qs.statement_end_offset < qs.statement_start_offset 
                                              THEN 0
                                             ELSE( qs.statement_end_offset - qs.statement_start_offset ) / 2 END + 2
                                           ), ''
                                       ), st.text
                                   ) + CHAR(13)+CHAR(10) + CHAR(13)+CHAR(10) FOR XML RAW, ELEMENTS) AS Tab2(Col1)
               OUTER APPLY sys.dm_exec_text_query_plan(qs.plan_handle,
                                                         qs.statement_start_offset,
                                                         qs.statement_end_offset) AS detqp
               OUTER APPLY (SELECT TRY_CONVERT(XML, detqp.query_plan) AS qXML) AS TabPlanXML
               WHERE qs.last_execution_time = TabIndexUsage.[Last time index(or a table if obj is not a Index_Statistic) was used]
               AND detqp.query_plan COLLATE Latin1_General_BIN2 LIKE '%' + REPLACE(REPLACE(a.StatsName, '[', ''), ']', '') + '%'
         )
         ELSE NULL
       END AS [Query plan associated with last usage],
       a.[Current number of rows on table], 
       a.[Number of rows on table at time statistic was updated],
       a.[Current number of modified rows since last update],
       Tab3.AutoUpdateThreshold,
       Tab3.AutoUpdateThresholdType
  FROM tempdb.dbo.Tab_GetStatisticInfo_FabianoAmorim AS a
 CROSS APPLY (SELECT CASE 
                       WHEN (SELECT compatibility_level 
                               FROM sys.databases 
                              WHERE QUOTENAME(name) = a.DatabaseName) >= 130 
                            AND 
                            COALESCE(a.unfiltered_rows, 0) >= 25001
                         THEN 'Dynamic'
                       ELSE 'Static'
                     END, 
                     CASE 
                       WHEN (SELECT compatibility_level 
                               FROM sys.databases 
                              WHERE QUOTENAME(name) = a.DatabaseName) >= 130
                            AND 
                            COALESCE(a.unfiltered_rows, 0) >= 25001
                         THEN CONVERT(BIGINT, SQRT(1000 * COALESCE(a.unfiltered_rows, 0)))
                       ELSE (CASE
				                           WHEN COALESCE(a.unfiltered_rows, 0) IS NULL THEN 0
				                           WHEN COALESCE(a.unfiltered_rows, 0) <= 500 THEN 501
				                           ELSE 500 + CONVERT(BIGINT, COALESCE(a.unfiltered_rows, 0) * 0.2)
			                          END)
                     END) AS Tab3(AutoUpdateThresholdType, AutoUpdateThreshold)
LEFT OUTER JOIN sys.dm_db_index_usage_stats
ON a.DatabaseID = dm_db_index_usage_stats.database_id
AND a.ObjectID = dm_db_index_usage_stats.object_id
AND (dm_db_index_usage_stats.index_id = CASE WHEN a.[statistic type] = 'Index_Statistic' THEN a.stats_id ELSE 1 END)
OUTER APPLY (SELECT MIN(Dt) FROM (VALUES(dm_db_index_usage_stats.last_user_seek), 
                                        (dm_db_index_usage_stats.last_user_scan)
                               ) AS t(Dt)) AS TabIndexUsage([Last time index(or a table if obj is not a Index_Statistic) was used])
 WHERE a.[Number of statistic data] = 1
   AND a.[Number of rows on table at time statistic was updated] <= 500
   AND a.[Current number of modified rows since last update] >= 1
ORDER BY [Current number of rows on table] DESC, 
         a.DatabaseName,
         a.TableName,
         a.KeyColumnName,
         a.StatsName
GO

/*
  Check 9 - Trace flag check - TF2371 (Changes the fixed update statistics threshold to a linear update statistics threshold.)
  By default, statistics are updated after 20% +500 rows of data have been modified, this may be too much for big tables.
  TF2371 can be used to reduce the number of modifications required for automatic updates to statistics to occur.
  
  It is important to update statistics on a regular basis through a scheduled job and leaving the auto update enabled as a safety.
*/

DECLARE @sqlmajorver INT, @sqlminorver int, @sqlbuild int
SELECT @sqlmajorver = CONVERT(int, (@@microsoftversion / 0x1000000) & 0xff),
	      @sqlminorver = CONVERT(int, (@@microsoftversion / 0x10000) & 0xff),
 	     @sqlbuild = CONVERT(int, @@microsoftversion & 0xffff);

DECLARE @min_compat_level tinyint
SELECT @min_compat_level = min([compatibility_level])
		from sys.databases

DECLARE @tracestatus TABLE(TraceFlag nVarChar(40)
                         , Status    tinyint
                         , GLOBAL    tinyint
                         , SESSION   tinyint)

INSERT INTO @tracestatus
EXEC ('DBCC TRACESTATUS')

SELECT 
  'Check 9 - Trace flag check - TF2371 (Changes the fixed update statistics threshold to a linear update statistics threshold.)' AS [Info],
  CASE 
    WHEN NOT EXISTS(SELECT TraceFlag
	                   FROM @tracestatus
	                   WHERE [Global] = 1 AND TraceFlag = 2371) /*TF2371 is not enabled*/
	        AND ((@sqlmajorver = 10 /*SQL2008*/ AND @sqlminorver = 50 /*50 = R2*/ AND @sqlbuild >= 2500 /*SP1*/) OR @sqlmajorver < 13 /*SQL2016*/)
      THEN '[Warning: Consider enabling TF2371 to change the 20pct fixed rate threshold for update statistics into a dynamic percentage rate]'
    WHEN NOT EXISTS (SELECT TraceFlag
			                  FROM @tracestatus
			                  WHERE [Global] = 1 AND TraceFlag = 2371) /*TF2371 is not enabled*/
			     AND (@sqlmajorver >= 13 AND @min_compat_level < 130) /*SQL Server is 2016(13.x) but there are DBs with compatibility level < 130*/
      THEN '[Warning: Some databases have a compatibility level < 130 (SQL2016). Consider enabling TF2371 to change the 20pct fixed rate threshold for update statistics into a dynamic percentage rate]'
    WHEN EXISTS(SELECT TraceFlag
			            FROM @tracestatus
			            WHERE [Global] = 1 
               AND TraceFlag = 2371) /*TF2371 is enabled*/
      THEN CASE
             WHEN (@sqlmajorver = 10 /*SQL2008*/ AND @sqlminorver = 50 /*50 = R2*/ AND @sqlbuild >= 2500 /*SP1*/)
                  OR (@sqlmajorver BETWEEN 11 /*SQL2012*/ AND 12 /*SQL2014*/)
                  OR (@sqlmajorver >= 13 /*SQL2016*/ AND @min_compat_level < 130 /*SQL2016*/) 
               THEN '[INFORMATION: TF2371 is enabled, this TF changes the fixed rate of the 20pct threshold for update statistics into a dynamic percentage rate]'
             WHEN @sqlmajorver >= 13 /*SQL2016*/ AND @min_compat_level >= 130 /*SQL2016*/
               THEN '[Warning: TF2371 is not needed in SQL 2016 and above when all databases are at compatibility level 130 and above]'
             ELSE '[Warning: Manually verify need to set a Non-default TF with current system build and configuration]'
           END
    ELSE 'OK'
  END AS [Comment]
GO

/*
  Check 10 - Check if there are statistics set as ascending/descending
*/

SELECT 'Check 30 - Check if there are statistics set as ascending/descending' AS [Info],
       a.DatabaseName,
       a.TableName,
       a.StatsName,
       a.KeyColumnName,
       a.[Statistic type],
       a.Statistic_Updated,
       a.[Current number of rows on table],
       a.[Leading column type],
       a.[Leading column type comment]
  FROM tempdb.dbo.Tab_GetStatisticInfo_FabianoAmorim a
WHERE a.[Current number of rows on table] > 0 /* Ignoring empty tables */
  AND a.[Number of statistic data] = 1
  AND a.[Leading column type] IN ('Ascending',  'Descending') 
ORDER BY a.[Current number of rows on table] DESC, 
         DatabaseName,
         TableName,
         KeyColumnName,
         StatsName
GO

/*
  Check 11 - Trace flag check - TF4139 (Enable automatically generated quick statistics (histogram amendment) regardless of key column status.)

  Check TF4139, TF4139 Enable automatically generated quick statistics (histogram amendment) regardless of key column status. 
  If trace flag 4139 is set, regardless of the leading statistics column status (ascending, descending, unknown or stationary), 
  the histogram used to estimate cardinality will be adjusted at query compile time.

  When fewer than 90 percent of the inserted rows have values that are beyond the highest RANGE_HI_KEY value in the histogram, 
  the column is considered stationary instead of ascending. 
  Therefore, the ascending key is not detected, and trace flags 2389 and 2390 that 
  are usually used to fix the ascending keys problem do not work. 
  This causes poor cardinality estimation when you use predicates that are beyond 
  the RANGE_HI_KEY value of the existing statistics.

  Note: This trace flag does not apply to CE version 70. Use trace flags 2389 and 2390 instead.
*/

/*
   Check if there are statistics with number of inserted rows that are beyond the highest RANGE_HI_KEY value in the histogram,
   and are still considered unknown or stationary... If so, queries trying to read those 
   recent rows would be benefitial of TF4139.
*/
IF OBJECT_ID('tempdb.dbo.#tmpCheck10') IS NOT NULL
  DROP TABLE #tmpCheck10
SELECT 
  [Number of statistic data],  TableName, StatsName, KeyColumnName, [Current number of rows on table],
  [Leading column type], 
  [Number of rows inserted above],
  [Number of rows inserted below],
  Tab1.[Percent of modifications], 
  [Number of modifications on key column since previous update],
  [Number of inserted rows on key column since previous update],
  [Number of deleted rows on key column since previous update]
INTO #tmpCheck10
FROM tempdb.dbo.Tab_GetStatisticInfo_FabianoAmorim
CROSS APPLY (SELECT CONVERT(DECIMAL(18, 2), ([Number of rows inserted above] / (CASE WHEN [Number of modifications on key column since previous update] = 0 THEN 1 ELSE [Number of modifications on key column since previous update] END * 1.0)) * 100.0)) AS Tab1([Percent of modifications])
WHERE [Number of statistic data] = 1
AND [Leading column type] IN ('unknown', 'stationary')
AND [Number of rows inserted above] > 0 
AND [Number of rows inserted below] = 0
AND [Current number of rows on table] > 1000 /* Ignoring small tables */
AND Tab1.[Percent of modifications] >= 50
ORDER BY [Current number of rows on table] DESC, 
         DatabaseName,
         TableName,
         KeyColumnName,
         StatsName

DECLARE @sqlmajorver INT, @sqlminorver int, @sqlbuild int
SELECT @sqlmajorver = CONVERT(int, (@@microsoftversion / 0x1000000) & 0xff),
	      @sqlminorver = CONVERT(int, (@@microsoftversion / 0x10000) & 0xff),
 	     @sqlbuild = CONVERT(int, @@microsoftversion & 0xffff);

DECLARE @tracestatus TABLE(TraceFlag nVarChar(40)
                         , Status    tinyint
                         , GLOBAL    tinyint
                         , SESSION   tinyint)

INSERT INTO @tracestatus
EXEC ('DBCC TRACESTATUS')

DECLARE @NumberOfTablesWithStationaryOrUnkown INT
SELECT @NumberOfTablesWithStationaryOrUnkown = COUNT(*) 
FROM #tmpCheck10

DECLARE @max_compat_level tinyint
SELECT @max_compat_level = max([compatibility_level])
		from sys.databases

SELECT 
  'Check 11 - Trace flag check - TF4139 (Enable automatically generated quick statistics (histogram amendment) regardless of key column status.)' AS [Info],
  CASE 
    WHEN EXISTS (SELECT TraceFlag
		               FROM @tracestatus
		               WHERE [Global] = 1 AND TraceFlag = 4139)
				THEN 
      CASE
        WHEN (@sqlmajorver = 11 /*SQL2012*/ AND @sqlbuild >= 5532 /*CU1 SP2*/)
					        OR (@sqlmajorver = 11 /*SQL2012*/ AND @sqlbuild >= 3431 /*CU10 SP1*/ AND @sqlbuild < 5058 /*SP2*/)
					        OR @sqlmajorver >= 12 /*SQL2014*/
				    THEN '[INFORMATION: TF4139 is enabled and will automatically generated quick statistics (histogram amendment) regardless of key column status]'
			     ELSE '[Warning: TF4139 only works starting with "CU10 for SQL2012 SP1", "CU1 for SQL2012 SP2" and "CU2 for SQL2014", no need to enabled it in older versions]'
			   END
    WHEN NOT EXISTS (SELECT TraceFlag
		                   FROM @tracestatus
		                   WHERE [Global] = 1 AND TraceFlag = 4139)
				THEN 
      CASE
        WHEN ((@sqlmajorver = 11 /*SQL2012*/ AND @sqlbuild >= 5532 /*CU1 SP2*/)
					        OR (@sqlmajorver = 11 /*SQL2012*/ AND @sqlbuild >= 3431 /*CU10 SP1*/ AND @sqlbuild < 5058 /*SP2*/)
					        OR @sqlmajorver >= 12 /*SQL2014*/) 
             AND (ISNULL(@NumberOfTablesWithStationaryOrUnkown,0) > 0)
             AND (@max_compat_level >= 120 /*SQL2014*/)
				    THEN '[Warning: Some databases have a compatibility level >= 120 (SQL2014). Found ' + CONVERT(VarChar, @NumberOfTablesWithStationaryOrUnkown) + ' stats with inserts beyond the highest RANGE_HI_KEY value in the histogram but still set to Stationary of Unknown. Consider enabling TF4139 to automatically generate quick statistics (histogram amendment) regardless of key column status]'
        ELSE 'OK'
      END
    ELSE 'OK'
  END AS [Comment]
GO

/*
  Check 12 - Trace flag check - TF2389 and TF2390 (Enable automatically generated quick statistics (histogram amendment))
  Check TF2389 and TF2390:
  TF2389 enable automatically generated quick statistics for ascending keys (histogram amendment).
  TF2390 enable automatically generated quick statistics regardless of the leading statistics column status (ascending, descending, unknown or stationary).

  Note: This trace flag does not apply to CE version 120 or above. Use trace flag 4139 instead.
*/

DECLARE @sqlmajorver INT, @sqlminorver int, @sqlbuild int
SELECT @sqlmajorver = CONVERT(int, (@@microsoftversion / 0x1000000) & 0xff),
	      @sqlminorver = CONVERT(int, (@@microsoftversion / 0x10000) & 0xff),
 	     @sqlbuild = CONVERT(int, @@microsoftversion & 0xffff);

DECLARE @min_compat_level tinyint
SELECT @min_compat_level = min([compatibility_level])
		from sys.databases

DECLARE @tracestatus TABLE(TraceFlag nVarChar(40)
                         , Status    tinyint
                         , GLOBAL    tinyint
                         , SESSION   tinyint)

INSERT INTO @tracestatus
EXEC ('DBCC TRACESTATUS')

SELECT 
  'Check 12 - Trace flag check - TF2389 and TF2390 (Enable automatically generated quick statistics (histogram amendment))' AS [Info],
  CASE 
    WHEN (SELECT COUNT(*)
		        FROM @tracestatus
		        WHERE [Global] = 1 AND TraceFlag IN (2389, 2390)) = 1
				THEN 
      CASE
        WHEN (@min_compat_level < 120 /*SQL2014*/)
				    THEN '[Warning: Only one TF is enabled, consider enabling both TF 2389 and 2390.]'
			     ELSE 'OK'
      END
    WHEN NOT EXISTS (SELECT TraceFlag
		                   FROM @tracestatus
		                   WHERE [Global] = 1 AND TraceFlag IN (2389, 2390))
				THEN 
      CASE
        WHEN (@min_compat_level < 120 /*SQL2014*/)
				    THEN '[Warning: Some databases have a compatibility level < 120 (SQL2014). Consider enabling TF 2389 and 2390 to automatically generate quick statistics (histogram amendment) regardless of key column status]'
        ELSE 'OK'
			   END
    ELSE 'OK'
  END AS [Comment]
GO

/*
  Check 13 - Trace flag check - TF4199, enables query optimizer changes released in SQL Server Cumulative Updates and Service Packs
  Check TF4199, TF4199 enables query optimizer changes released in SQL Server Cumulative Updates and Service Packs.

  Query Optimizer fixes released for previous SQL Server versions under trace flag 4199 
  become automatically enabled in the default compatibility level of a newer SQL Server version.
  Post-RTM Query Optimizer fixes still need to be explicitly enabled via QUERY_OPTIMIZER_HOTFIXES option in ALTER DATABASE SCOPED CONFIGURATION 
  or via trace flag 4199.

  You STILL NEED TF4199 (or DB scope config) to get post-RTM Query Optimizer fixes.
  Here is a quick sample of fix only applied under TF4199: 
  FIX: Slow query performance when using query predicates with UPPER, LOWER or RTRIM with default CE in SQL Server 2017 and 2019
  https://support.microsoft.com/en-us/topic/kb4538497-fix-slow-query-performance-when-using-query-predicates-with-upper-lower-or-rtrim-with-default-ce-in-sql-server-2017-and-2019-5619b55c-b0b4-0a8e-2bce-2ffe6b7eb70e
*/

DECLARE @sqlmajorver INT, @sqlminorver int, @sqlbuild int
SELECT @sqlmajorver = CONVERT(int, (@@microsoftversion / 0x1000000) & 0xff)

DECLARE @tracestatus TABLE(TraceFlag nVarChar(40)
                         , Status    tinyint
                         , GLOBAL    tinyint
                         , SESSION   tinyint)

INSERT INTO @tracestatus
EXEC ('DBCC TRACESTATUS')

SELECT 
  'Check 13 - Trace flag check - TF4199, enables query optimizer changes released in SQL Server Cumulative Updates and Service Packs' AS [Info],
  CASE 
    WHEN NOT EXISTS(SELECT TraceFlag
		                    FROM @tracestatus
		                   WHERE [Global] = 1 AND TraceFlag = 4199)
         AND (@sqlmajorver >= 13 /*SQL2016*/)
				THEN '[Warning: Consider enabling TF4199 or QUERY_OPTIMIZER_HOTFIXES option in ALTER DATABASE SCOPED CONFIGURATION. This will enable query optimizer changes released in SQL Server Cumulative Updates and Service Packs]'
    WHEN NOT EXISTS(SELECT TraceFlag
		                    FROM @tracestatus
		                   WHERE [Global] = 1 AND TraceFlag = 4199)
				THEN '[Warning: Consider enabling TF4199 to enable query optimizer changes released in SQL Server Cumulative Updates and Service Packs]'
    ELSE 'OK'
  END AS [Comment]
GO

/*
  Check 14 - Do I have filtered statistics?
  Check to see if there are filtered statistics, if so, that may indicate smart users are on.

  Things to look for:

  1 - Filtered stats may don't play well with ad-hoc queries and constant values, for those cases,
      it may be necessary to use "WHERE Col = (Select 1)" to be able to avoid auto/forced param.
  2 - Filtered stats may take a lot of time to auto-update.
  3 - Filtered stats may not be used due to parameter sniffing ... May necessary to add OPTION (RECOMPILE) or use
      dynamic queries.
*/

SELECT 
  'Check 14 - Do I have filtered statistics?' AS [Info],
  [Number of statistic data],  
  TableName, 
  StatsName, 
  KeyColumnName, 
  [Statistic type],
  [Statistic_Updated],
  [Current number of rows on table],
  [Number of rows on table at time statistic was updated],
  unfiltered_rows AS [Number of rows on table at time statistics was updated ignoring filter],
  filter_definition
FROM tempdb.dbo.Tab_GetStatisticInfo_FabianoAmorim
WHERE [Number of statistic data] = 1
AND has_filter = 1
ORDER BY [Current number of rows on table] DESC, 
         DatabaseName,
         TableName,
         KeyColumnName,
         StatsName
GO


/*
  Check 15 - Do I have data skew issues with limited histogram causing poor estimations?


  The first thing you should do it to update the statistic with fullscan, 
  as this may provide a better histogram, if this do not help, try filtered stats.
  
  Filtered stats can help witht those columns, good candidates for this are:
  * Big tables (usually over 1mi rows)
  * Columns with lot's of unique values (low density)
  * Statistics already using almost all steps available (200 + 1 for NULL)

  Kimberly’s scripts can help to analyze analyzes data skew and identify 
  where you can create filtered statistics to provide more information to the Query Optimizer.
  https://www.sqlskills.com/blogs/kimberly/sqlskills-procs-analyze-data-skew-create-filtered-statistics/

  Example:

  -- Step 1 - analyze a specific table/column
  USE Northwind
  GO
  EXEC sp_SQLskills_AnalyzeColumnSkew
    @schemaname = 'dbo', 
    @objectname = 'Order_DetailsBig',
    @columnname = 'Shipped_Date',
    @difference	= 1000,
  				  -- Looking for the minimum difference between average
						  -- and biggest difference in that step
    @factor = 2.5,
						  -- Looking for the minimum factor of the difference
						  -- against the average
    @numofsteps = 1,
						  -- This is the minimum number of steps that have to 
						  -- have this @difference or @factor (or both)
    @percentofsteps = 1
						  -- This is the minimum PERCENT of steps that have to 
						  -- have this @difference or @factor (or both)

  -- Results 
  -------------------------------------------------------------------------------------------------------------
  Begin processing @schemaname = [dbo], @objectname = [Order_DetailsBig], @columnname = [Shipped_Date].
  Table: [dbo].[Order_DetailsBig], column: [Shipped_Date] has 2 rows (of 125) with a greater difference than 1000. 
  This means that there are 2 steps that will result in row estimations that are off by more than 1000. 
  Just analyzing step differences, this table has 1.60 percent skew (minimum of 1 percent required by parameter). 
  This table shows signs of skew based on this criteria. You should consider filtered statistics on this column to help cardinality estimates.
  Caution: Changing any part of an object name could break scripts and stored procedures.
  Either parameter @keeptable = 'TRUE' was chosen OR at least one of your criteria showed skew. 
  As a result, we saved the table used for histogram analysis as [tempdb]..[SQLskills_HistogramAnalysisOf_Northwind_dbo_Order_DetailsBig_Shipped_Date]. 
  This table will need to be manually dropped or will remain in tempdb until it is recreated. 
  If this procedure is run again, this table will be replaced (if @keeptable = 'TRUE') but it will not be dropped unless you drop it.
  -------------------------------------------------------------------------------------------------------------

  -- Step 2 (optional) - Check tables with skewed data
  EXEC [sp_SQLskills_HistogramTempTables] @management = 'QUERY'
  EXEC [sp_SQLskills_HistogramTempTables] @management = 'DROP'

  -- Step 3 - Create filtered stats on columns you identified to be worthy
  USE Northwind
  GO
  EXEC [sp_SQLskills_CreateFilteredStats]
    @schemaname = 'dbo', 
    @objectname = 'Order_DetailsBig', 
    @columnname = 'Shipped_Date',
	   @filteredstats	= 10,
						-- this is the number of filtered statistics
						-- to create. For simplicity, you cannot
						-- create more filtered stats than there are
						-- steps within the histogram (mostly because
						-- not all data is uniform). Maybe in V2.
						-- And, 10 isn't necessarily 10. Because the 
						-- number might not divide easily there are 
						-- likely to be n + 1. And, if @everincreasing
						-- is 1 then you'll get n + 2. 
						-- (the default of 10 may create 11 or 12 stats)
    @fullscan = 'FULLSCAN'
  GO

  -- Step 4 - Check stats
  select * from sys.stats
  cross apply sys.dm_db_stats_properties(stats.object_id, stats.stats_id)
  where stats.object_id = OBJECT_ID('Order_DetailsBig')
  GO

  -- Step 5 (optional) - Drop column stats
  USE Northwind
  GO
  EXEC [dbo].[sp_SQLskills_DropAllColumnStats] 
    @schemaname = 'dbo', 
    @objectname = 'Order_DetailsBig', 
    @columnname = 'Shipped_Date',
    @DropAll = 'true'

  -- Step 6 (optional) - Test all key columns on DB
  -- Depending on the table sizes this may take a while to run
  USE Northwind
  GO
  EXEC sp_SQLskills_AnalyzeAllLeadingIndexColumnSkew 
    @schemaname = NULL, 
    @objectname = NULL,
    @difference	= 1000,
    @factor = 2.5,
    @numofsteps = 1,
    @percentofsteps = 1

  Note 1: Kimberly’s scripts will only analyze data if a base index is available, this may be good for almost all cases
  since we're expecting you to have indexes on filtered columns, but, you may want to check it manually for non-indexed columns

  Note 2: Kimberly’s scripts will NOT check for wrong estimations due to data that doesn't exist on statistic... In other words,  
  if estimated number of rows is 1000 and actual number of rows is 0, it will not identify those cases.
*/

SELECT 
  'Check 15 - Do I have data skew issues with limited histogram causing poor estimations?' AS [Info],
  [Number of statistic data],  
  TableName, 
  StatsName, 
  KeyColumnName, 
  [Statistic type],
  [Statistic_Updated],
  [Current number of rows on table],
  user_seeks + user_scans + user_lookups AS [Number of reads on index/table since last restart],
  user_seeks + user_scans + user_lookups / 
  CASE DATEDIFF(hh, (SELECT create_date FROM sys.databases WHERE name = 'tempdb'), getdate())
    WHEN 0 THEN 1
    ELSE DATEDIFF(hh, (SELECT create_date FROM sys.databases WHERE name = 'tempdb'), getdate())
  END AS [Avg of reads per hour],
  user_updates AS [Number of modifications on index/table since last restart],
  user_updates /
  CASE DATEDIFF(hh, (SELECT create_date FROM sys.databases WHERE name = 'tempdb'), getdate())
    WHEN 0 THEN 1
    ELSE DATEDIFF(hh, (SELECT create_date FROM sys.databases WHERE name = 'tempdb'), getdate())
  END AS [Avg of modifications per hour],
  range_scan_count AS [Number of range scans since last restart/rebuild],
  page_latch_wait_count AS [Number of page latch since last restart/rebuild],
  page_io_latch_wait_count AS [Number of page I/O latch since last restart/rebuild],
  [Number of steps on histogram],
  TF2388_Density AS [Key column density],
  1.0 / CASE TF2388_Density WHEN 0 THEN 1 ELSE TF2388_Density END AS [Unique values on key column],
  CASE 
    WHEN ([Number of steps on histogram] >= 190) 
      OR (1.0 / CASE TF2388_Density WHEN 0 THEN 1 ELSE TF2388_Density END) >= 1000
      THEN 'This looks like a good candidate to test data skew using Kimberly’s scripts'
    ELSE 'Looks like this is not a good candidate to test data skew using Kimberly’s scripts, but, you know the data, so, final decision is yours.'
  END AS [Comment],
  CASE 
    WHEN ([Number of steps on histogram] >= 190) 
      OR (1.0 / CASE TF2388_Density WHEN 0 THEN 1 ELSE TF2388_Density END) >= 1000
      THEN 'USE ' + DatabaseName + 
           '; EXEC sp_SQLskills_AnalyzeColumnSkew @schemaname = ' + 
           '''' + REPLACE(REPLACE(SchemaName, '[', ''), ']', '') + '''' +
           ', @objectname = ' + 
           '''' +REPLACE(REPLACE(TableName, '[', ''), ']', '') + '''' +
           ', @columnname = ' +
           '''' +REPLACE(REPLACE(KeyColumnName, '[', ''), ']', '') + '''' +
           ', @difference	= 1000, @factor = NULL, @numofsteps = NULL, @percentofsteps = 1;'
    ELSE ''
  END AS [Command to test Kimberly’s script]
FROM tempdb.dbo.Tab_GetStatisticInfo_FabianoAmorim AS a
WHERE [Number of statistic data] = 1
AND [Statistic type] = 'Index_Statistic'
ORDER BY [Current number of rows on table] DESC, 
         DatabaseName,
         TableName,
         KeyColumnName,
         StatsName
GO


/*
  Check 16 - Are filtered stats are out of date ?
  Filtered stats might become seriously out of date
  https://www.sqlskills.com/blogs/kimberly/filtered-indexes-and-filtered-stats-might-become-seriously-out-of-date/
*/

SELECT 
  'Check 16 - Are filtered stats are out of date ?' AS [Info],
  [Number of statistic data],  
  TableName, 
  StatsName, 
  KeyColumnName, 
  [Statistic type],
  filter_definition,
  [Statistic_Updated],
  DATEDIFF(hh,[Statistic_Updated],GETDATE()) AS [Hours since last update],
  CASE 
    WHEN DATEDIFF(hh,[Statistic_Updated], GETDATE()) > 24 THEN 
         'It has been more than 24 hours [' + CONVERT(VarChar(4), DATEDIFF(mi,[Statistic_Updated],GETDATE()) / 60 / 24) + 'd ' + CONVERT(VarChar(4), DATEDIFF(mi,[Statistic_Updated],GETDATE()) / 60 % 24) + 'hr '
         + CONVERT(VarChar(4), DATEDIFF(mi,[Statistic_Updated],GETDATE()) % 60) + 'min' 
         + '] since last update statistic.'
    ELSE 'OK'
  END AS [Comment],
  [Current number of rows on table], 
  [Number of rows on table at time statistic was updated],
  unfiltered_rows AS [Number of rows on table at time statistics was updated ignoring filter],
  [Current number of modified rows since last update],
  Tab3.AutoUpdateThreshold,
  Tab3.AutoUpdateThresholdType,
  CONVERT(DECIMAL(18, 2), (a.[Current number of modified rows since last update] / (Tab3.AutoUpdateThreshold * 1.0)) * 100.0) AS [Percent of threshold]
FROM tempdb.dbo.Tab_GetStatisticInfo_FabianoAmorim AS a
CROSS APPLY (SELECT CASE 
                       WHEN (SELECT compatibility_level 
                               FROM sys.databases 
                              WHERE QUOTENAME(name) = a.DatabaseName) >= 130 
                            AND 
                            COALESCE(a.unfiltered_rows, 0) >= 25001
                         THEN 'Dynamic'
                       ELSE 'Static'
                     END, 
                     CASE 
                       WHEN (SELECT compatibility_level 
                               FROM sys.databases 
                              WHERE QUOTENAME(name) = a.DatabaseName) >= 130
                            AND 
                            COALESCE(a.unfiltered_rows, 0) >= 25001
                         THEN CONVERT(BIGINT, SQRT(1000 * COALESCE(a.unfiltered_rows, 0)))
                       ELSE (CASE
				                           WHEN COALESCE(a.unfiltered_rows, 0) IS NULL THEN 0
				                           WHEN COALESCE(a.unfiltered_rows, 0) <= 500 THEN 501
				                           ELSE 500 + CONVERT(BIGINT, COALESCE(a.unfiltered_rows, 0) * 0.2)
			                          END)
                     END) AS Tab3(AutoUpdateThresholdType, AutoUpdateThreshold)
WHERE [Number of statistic data] = 1
AND has_filter = 1
ORDER BY [Current number of rows on table] DESC, 
         DatabaseName,
         TableName,
         KeyColumnName,
         StatsName
GO

/*
  Check 17 - Statistics on views are only created and used when using noexpand

  SQL Server can create statistics automatically to assist with cardinality estimation 
  and cost-based decision-making during query optimization. 
  This feature works with indexed views as well as base tables, 
  but only if the view is explicitly named in the query and the NOEXPAND hint is specified.

  If you have query using an indexed view, it maybe worthy to review all queries using it
  and make sure you're using NOEXPAND to have more accurate estimates.
*/

IF OBJECT_ID('tempdb.dbo.#db') IS NOT NULL
  DROP TABLE #db

IF OBJECT_ID('tempdb.dbo.#tmp_IndexedViews1') IS NOT NULL
  DROP TABLE #tmp_IndexedViews1

CREATE TABLE #tmp_IndexedViews1 (DatabaseName NVarChar(800),
                                 SchemaName   NVarChar(800),
                                 ViewName     NVarChar(2000),
                                 IndexName    NVarChar(800))

IF OBJECT_ID('tempdb.dbo.#tmp_IndexedViews2') IS NOT NULL
  DROP TABLE #tmp_IndexedViews2

CREATE TABLE #tmp_IndexedViews2 (DatabaseName                    NVarChar(800),
                                 SchemaName                      NVarChar(800),
                                 ObjectName                      NVarChar(800),
                                 [Type of object]                NVarChar(800),
                                 [Does it has noexpand keyword?] VarChar(3),
                                 ViewName                        NVarChar(2000),
                                 IndexName                       NVarChar(800),
                                 [Object code definition]        XML)

SELECT d1.[name] into #db
FROM sys.databases d1
where d1.state_desc = 'ONLINE' and is_read_only = 0
and d1.database_id in (SELECT DISTINCT DatabaseID FROM tempdb.dbo.Tab_GetStatisticInfo_FabianoAmorim)

DECLARE @SQL VarChar(MAX)
declare @database_name sysname
DECLARE @ErrMsg VarChar(8000)

DECLARE c_databases CURSOR read_only FOR
    SELECT [name] FROM #db
OPEN c_databases

FETCH NEXT FROM c_databases
into @database_name
WHILE @@FETCH_STATUS = 0
BEGIN
  SET @ErrMsg = 'Checking indexed views usage on DB - [' + @database_name + ']'
  RAISERROR (@ErrMsg, 10, 1) WITH NOWAIT

  SET @SQL = 'use [' + @database_name + ']; 
              select 
                    QUOTENAME(DB_NAME()) AS DatabaseName, 
                    QUOTENAME(OBJECT_SCHEMA_NAME(si.object_id)) AS SchemaName,
                    QUOTENAME(OBJECT_NAME(si.object_id)) AS [ViewName],
                    QUOTENAME(si.name) AS IndexName
                from sys.indexes AS si
                inner join sys.views AS sv
                    ON si.object_id = sv.object_id'

  /*SELECT @SQL*/
  INSERT INTO #tmp_IndexedViews1
  EXEC (@SQL)

  SET @SQL = 'use [' + @database_name + ']; 
              ;WITH CTE_1
              AS
              (
                select 
                    QUOTENAME(DB_NAME()) + ''.'' + 
                    QUOTENAME(OBJECT_SCHEMA_NAME(si.object_id)) + ''.'' + 
                    QUOTENAME(OBJECT_NAME(si.object_id)) AS [ViewName],
                    OBJECT_NAME(si.object_id) AS tmpViewName,
                    QUOTENAME(si.name) AS IndexName
                from sys.indexes AS si
                inner join sys.views AS sv
                    ON si.object_id = sv.object_id
              )
              SELECT QUOTENAME(DB_NAME()) AS DatabaseName, 
                     QUOTENAME(ss.name) AS [SchemaName], 
                     QUOTENAME(so.name) AS [ObjectName], 
                     so.type_desc [Type of object],
                     CASE 
                       WHEN PATINDEX(''%noexpand%'', LOWER(sm.[definition]) COLLATE DATABASE_DEFAULT) > 1 THEN ''Yes''
                       ELSE ''No''
                     END AS [Does it has noexpand keyword?],
                     t.ViewName,
                     t.IndexName,
                     CONVERT(XML, Tab1.Col1) AS [Object code definition]
              FROM CTE_1 AS t
              INNER JOIN sys.sql_modules sm
              ON PATINDEX(''%'' + t.tmpViewName + ''%'', LOWER(sm.[definition]) COLLATE DATABASE_DEFAULT) > 1
              INNER JOIN sys.objects so 
              ON sm.[object_id] = so.[object_id]
              INNER JOIN sys.schemas ss 
              ON so.[schema_id] = ss.[schema_id]
              CROSS APPLY (SELECT CHAR(13)+CHAR(10) + sm.[definition] + CHAR(13)+CHAR(10) FOR XML RAW, ELEMENTS) AS Tab1(Col1)
              WHERE OBJECTPROPERTY(sm.[object_id],''IsMSShipped'') = 0
              AND OBJECT_NAME(sm.object_id) <> t.tmpViewName
              OPTION (FORCE ORDER)'

  /*SELECT @SQL*/
  INSERT INTO #tmp_IndexedViews2
  EXEC (@SQL)
  
  FETCH NEXT FROM c_databases
  into @database_name
END
CLOSE c_databases
DEALLOCATE c_databases
GO
SELECT 'Check 17 - Statistics on views are only created and used when using noexpand' AS [Info],
       * 
FROM #tmp_IndexedViews1
GO

SELECT 'Check 17 - Statistics on views are only created and used when using noexpand' AS [Info],
       DatabaseName,
       SchemaName,
       ObjectName,
       [Type of object],
       [Does it has noexpand keyword?],
       CASE [Does it has noexpand keyword?]
         WHEN 'Yes' THEN 'Code looks good, but, please double check it to confirm that noexpand is really used, it may be in a commented text or something else.'
         ELSE 'Warning - Indexed view is referenced but NOEXPAND is not used, make sure you add it to benefit of statistics'
       END AS [Comment],
       ViewName,
       [Object code definition]
FROM #tmp_IndexedViews2
GO



/*
  Check 18 - Database seetings
*/

SELECT 'Check 18 - Database seetings' AS [Info], 
       [name] AS DatabaseName,
       is_auto_create_stats_on,
       CASE 
         WHEN is_auto_create_stats_on = 0 
         THEN 'Warning - Database [' + [name] + '] has auto-create-stats disabled. SQL Server uses statistics to build better execution plans, and without the ability to automatically create more, performance may suffer.'
         ELSE 'OK'
       END [Auto create stats comment],
       is_auto_update_stats_on,
       CASE 
         WHEN is_auto_update_stats_on = 0 
         THEN 'Warning - Database [' + [name] + '] has auto-update-stats disabled. SQL Server uses statistics to build better execution plans, and without the ability to automatically update them, performance may suffer.'
         ELSE 'OK'
       END [Auto update stats comment],
       is_auto_update_stats_async_on,
       CASE 
         WHEN is_auto_update_stats_async_on = 1 
         THEN 'Information - Database [' + [name] + '] has auto-update-stats-async enabled. When SQL Server gets a query for a table with out-of-date statistics, it will run the query with the stats it has - while updating stats to make later queries better. The initial run of the query may suffer, though.'
         ELSE 'OK'
       END [Auto update stats async comment 1],
       CASE 
         WHEN is_auto_update_stats_on = 0 AND is_auto_update_stats_async_on = 1
         THEN 'Warning - Database [' + [name] + '] Database [' + [name] + '] have Auto_Update_Statistics_Asynchronously ENABLED while Auto_Update_Statistics is DISABLED. If asynch auto statistics update is intended, also enable Auto_Update_Statistics.'
         ELSE 'OK'
       END [Auto update stats async comment 2],
       is_date_correlation_on,
       CASE 
         WHEN is_date_correlation_on = 1
         THEN 'Warning - Database [' + [name] + '] has date correlation enabled. This is not a default setting, and it has some performance overhead. Very unlikely it is really being useful, check if indexed views it uses are there but not really being used. If there is date correlation, you may get better performance by beating developers to make them to specify implied date boundaries.'
         ELSE 'OK'
       END [Date correlation optimization comment]
FROM sys.databases
WHERE database_id in (SELECT DISTINCT DatabaseID FROM tempdb.dbo.Tab_GetStatisticInfo_FabianoAmorim)


/*
  Check 19 - Check if incremental setting on DB should be set to ON

  The default value is OFF, which means stats are combined for all partitions.
  When ON, the statistics are created and updated per partition whenever incremental stats are supported.

  Applies to: SQL Server 2014 (12.x) and higher builds.
*/

DECLARE @sqlmajorver INT, @sqlminorver int, @sqlbuild int
SELECT @sqlmajorver = CONVERT(int, (@@microsoftversion / 0x1000000) & 0xff)
IF @sqlmajorver >= 13 /*SQL2014*/
BEGIN
  EXEC ('
  SELECT ''Check 19 - Check if incremental setting on DB should be set to ON'' AS [Info], 
         [name] AS DatabaseName,
         is_auto_create_stats_incremental_on,
         CASE 
           WHEN (is_auto_create_stats_incremental_on = 0)
            AND EXISTS(SELECT DISTINCT DatabaseID 
                         FROM tempdb.dbo.Tab_GetStatisticInfo_FabianoAmorim 
                        WHERE Tab_GetStatisticInfo_FabianoAmorim.IsTablePartitioned = 1
                          AND Tab_GetStatisticInfo_FabianoAmorim.DatabaseID = databases.database_id)
           THEN ''Warning - Database ['' + [name] + ''] has partitioned tables and auto-incremental-stats is disabled. Consider enabling it to allow SQL created and update stats per partition.''
           WHEN NOT EXISTS(SELECT DISTINCT DatabaseID 
                             FROM tempdb.dbo.Tab_GetStatisticInfo_FabianoAmorim 
                            WHERE Tab_GetStatisticInfo_FabianoAmorim.IsTablePartitioned = 1
                              AND Tab_GetStatisticInfo_FabianoAmorim.DatabaseID = databases.database_id)
           THEN ''Information - Database ['' + [name] + ''] does not have partitioned tables, check is not relevant.''
           ELSE ''OK''
         END [Auto create stats incremental comment]
  FROM sys.databases
  WHERE database_id in (SELECT DISTINCT DatabaseID 
                          FROM tempdb.dbo.Tab_GetStatisticInfo_FabianoAmorim)
  ')
END
ELSE
BEGIN
  SELECT 'Check 19 - Check if incremental setting on DB should be set to ON' AS [Info], 
         'Check is not relevant on this SQL version as Incremental stats only applies to SQL Server 2014 (12.x) and higher builds.' AS [Auto create stats incremental comment]
END
GO

/*
  Check 20 - Check if there are partitioned tables with indexes or statistics not using incremental

*/

DECLARE @sqlmajorver INT, @sqlminorver int, @sqlbuild int
SELECT @sqlmajorver = CONVERT(int, (@@microsoftversion / 0x1000000) & 0xff)
IF @sqlmajorver >= 13 /*SQL2014*/
BEGIN
  IF OBJECT_ID('tempdb.dbo.#db') IS NOT NULL
    DROP TABLE #db

  IF OBJECT_ID('tempdb.dbo.#tmp_StatsOnPartitionedTables') IS NOT NULL
    DROP TABLE #tmp_StatsOnPartitionedTables

  CREATE TABLE #tmp_StatsOnPartitionedTables (DatabaseID Int,
                                              ObjectID   Int,
                                              Stats_ID    INT,
                                              is_incremental BIT)

  SELECT d1.[name] INTO #db
  FROM sys.databases d1
  where d1.state_desc = 'ONLINE' and is_read_only = 0
  and d1.database_id in (SELECT DISTINCT DatabaseID FROM tempdb.dbo.Tab_GetStatisticInfo_FabianoAmorim)

  DECLARE @SQL VarChar(MAX)
  declare @database_name sysname
  DECLARE @ErrMsg VarChar(8000)

  DECLARE c_databases CURSOR read_only FOR
      SELECT [name] FROM #db
  OPEN c_databases

  FETCH NEXT FROM c_databases
  into @database_name
  WHILE @@FETCH_STATUS = 0
  BEGIN
    SET @ErrMsg = 'Checking incremental stats on DB - [' + @database_name + ']'
    RAISERROR (@ErrMsg, 10, 1) WITH NOWAIT

    SET @SQL = 'use [' + @database_name + ']; 
                SELECT t.DatabaseID, 
                       t.ObjectID, 
                       t.Stats_ID,
                       stats.is_incremental
                FROM tempdb.dbo.Tab_GetStatisticInfo_FabianoAmorim AS t
                INNER JOIN sys.stats
                ON stats.OBJECT_ID = t.ObjectID
                AND stats.stats_id = t.Stats_ID
                WHERE [Number of statistic data] = 1
                AND IsTablePartitioned = 1
                AND DatabaseName = QUOTENAME(DB_NAME())'

    /*SELECT @SQL*/
    INSERT INTO #tmp_StatsOnPartitionedTables
    EXEC (@SQL)
  
    FETCH NEXT FROM c_databases
    into @database_name
  END
  CLOSE c_databases
  DEALLOCATE c_databases

  SELECT 'Check 20 - Check if there are partitioned tables with indexes or statistics not using incremental' AS [Info],
         a.DatabaseName,
         a.TableName,
         a.StatsName,
         a.KeyColumnName,
         a.[Current number of rows on table],
         a.[Statistic type],
         a.IsTablePartitioned,
         #tmp_StatsOnPartitionedTables.is_incremental,
         CASE
           WHEN #tmp_StatsOnPartitionedTables.is_incremental = 0
           THEN 'Warning - Table is partitioned but statistic is not set to incremental. Rebuild the index using "WITH(STATISTICS_INCREMENTAL=ON)" or update the stats using "WITH RESAMPLE, INCREMENTAL = OFF"'
           ELSE 'OK'
         END AS [Comment],
         CASE
           WHEN #tmp_StatsOnPartitionedTables.is_incremental = 0 AND a.[Statistic type] = 'Index_Statistic'
           THEN 'ALTER INDEX ' + a.StatsName + ' ON ' + a.DatabaseName + '.' + a.SchemaName + '.' + a.TableName + 
                ' REBUILD WITH(STATISTICS_INCREMENTAL=ON, ' + 
                CASE 
                  WHEN CONVERT(VarChar(200), SERVERPROPERTY('Edition')) LIKE 'Developer%'
                    OR CONVERT(VarChar(200), SERVERPROPERTY('Edition')) LIKE 'Enterprise%' THEN ' ONLINE=ON)'
                  ELSE ' ONLINE=OFF)'
                END
           WHEN #tmp_StatsOnPartitionedTables.is_incremental = 0 AND a.[Statistic type] <> 'Index_Statistic'
           THEN 'UPDATE STATISTICS ' + a.DatabaseName + '.' + a.SchemaName + '.' + a.TableName + ' ' + a.StatsName + 
                ' WITH RESAMPLE, INCREMENTAL = ON;'
           ELSE 'OK'
         END AS [Command to implement incremental]
  FROM tempdb.dbo.Tab_GetStatisticInfo_FabianoAmorim AS a
  INNER JOIN #tmp_StatsOnPartitionedTables
  ON #tmp_StatsOnPartitionedTables.DatabaseID = a.DatabaseID
  AND #tmp_StatsOnPartitionedTables.ObjectID = a.ObjectID
  AND #tmp_StatsOnPartitionedTables.Stats_ID = a.Stats_ID
  WHERE [Number of statistic data] = 1
  ORDER BY a.[Current number of rows on table] DESC, 
           a.DatabaseName,
           a.TableName,
           a.KeyColumnName,
           a.StatsName
END
ELSE
BEGIN
  SELECT 'Check 20 - Check if there are partitioned tables with indexes or statistics not using incremental' AS [Info], 
         'Check is not relevant on this SQL version as Incremental stats only applies to SQL Server 2014 (12.x) and higher builds.' AS [Auto create stats incremental comment]
END
GO

/*
  Check 21 - Check TF11024, TF11024 enables triggering auto update of statistics when the modification count of any partition exceeds the local threshold.
  
  Check if table is partitioned, if so, we may need to use TF11024 to change
  the way SQL triggers auto update stats to trigger the auto update of statistics when 
  the modification count of any partition exceeds the local threshold.
  When this trace flag is enabled, the modification count of the root node is kept as the sum of modification counts of all partitions.
  https://support.microsoft.com/en-us/topic/kb4041811-fix-automatic-update-of-incremental-statistics-is-delayed-in-sql-server-2014-2016-and-2017-9a0043d9-f911-798a-f971-0a7aae696119
  
  Note: This trace flag applies to SQL Server 2014 (12.x) SP3, SQL Server 2016 (13.x) SP2, SQL Server 2017 (14.x) CU3, and higher builds.
*/

DECLARE @sqlmajorver INT, @sqlbuild int
SELECT @sqlmajorver = CONVERT(int, (@@microsoftversion / 0x1000000) & 0xff),
 	     @sqlbuild = CONVERT(int, @@microsoftversion & 0xffff);

DECLARE @min_compat_level tinyint
SELECT @min_compat_level = min([compatibility_level])
		from sys.databases

DECLARE @tracestatus TABLE(TraceFlag nVarChar(40)
                         , Status    tinyint
                         , GLOBAL    tinyint
                         , SESSION   tinyint)

INSERT INTO @tracestatus
EXEC ('DBCC TRACESTATUS')

IF EXISTS(SELECT * FROM tempdb.dbo.Tab_GetStatisticInfo_FabianoAmorim AS a WHERE a.IsTablePartitioned = 1)
BEGIN
  SELECT 
    'Check 21 - Check TF11024, TF11024 enables triggering auto update of statistics when the modification count of any partition exceeds the local threshold.' AS [Info],
    CASE 
      WHEN NOT EXISTS(SELECT TraceFlag
	                     FROM @tracestatus
	                     WHERE [Global] = 1 AND TraceFlag = 11024) /*TF11024 is not enabled*/
	          AND (
                   (@sqlmajorver = 12 /*SQL2014*/ AND @sqlbuild >= 6024 /*SP3*/) 
                OR (@sqlmajorver = 13 /*SQL2016*/ AND @sqlbuild >= 5026 /*SP2*/)
                OR (@sqlmajorver = 14 /*SQL2017*/ AND @sqlbuild >= 3015 /*CU3*/)
                OR (@sqlmajorver > 14 /*SQL2019*/)
               )
        THEN 'Warning: If incremental statistics is used, consider enabling TF11024 to enables auto update of statistics when the modification count of any partition exceeds the local threshold.'
      ELSE 'OK'
    END [Comment]
END
ELSE
BEGIN
  SELECT 
    'Check 21 - Check TF11024, TF11024 enables triggering auto update of statistics when the modification count of any partition exceeds the local threshold.' AS [Info],
    'There are no partitioned tables, check is not relevant.' AS [Comment]
END
GO

/*
  Check 22 - Estimate how often auto update statistic would be triggered
*/

SELECT 'Check 22 - Estimate how often auto update statistic would be triggered' AS [Info],
       a.DatabaseName,
       a.TableName,
       a.StatsName,
       a.KeyColumnName,
       a.[Statistic_Updated],
       TabIndexUsage.[Last time index(or a table if obj is not a Index_Statistic) was used],
       a.[Current number of rows on table], 
       a.[Number of rows on table at time statistic was updated],
       a.unfiltered_rows AS [Number of rows on table at time statistics was updated ignoring filter],
       a.[Current number of modified rows since last update],
       Tab3.AutoUpdateThreshold,
       Tab3.AutoUpdateThresholdType,
	      CONVERT(DECIMAL(18, 2), (a.[Current number of modified rows since last update] / (Tab3.AutoUpdateThreshold * 1.0)) * 100.0) AS [Percent of threshold],
       TabModificationsPerMinute.[Avg modifications per minute based on existing update stats intervals],
       CONVERT(VarChar, CONVERT(INT, Tab3.AutoUpdateThreshold / TabModificationsPerMinute.[Avg modifications per minute based on existing update stats intervals])) AS [Estimated frequency of auto update stats],
       'Considering that statistic has Avg of ' + 
       CONVERT(VarChar, TabModificationsPerMinute.[Avg modifications per minute based on existing update stats intervals]) + 
       ' modifications per minute and update stat threshold of ' + 
       CONVERT(VarChar, Tab3.AutoUpdateThreshold) + 
       ', estimated frequency of auto update stats is every ' + CONVERT(VarChar, CONVERT(INT, Tab3.AutoUpdateThreshold / TabModificationsPerMinute.[Avg modifications per minute based on existing update stats intervals])) + 
       ' minutes.'
       AS [Comment 1]
  FROM tempdb.dbo.Tab_GetStatisticInfo_FabianoAmorim AS a
 CROSS APPLY (SELECT CASE 
                       WHEN (SELECT compatibility_level 
                               FROM sys.databases 
                              WHERE QUOTENAME(name) = a.DatabaseName) >= 130 
                            AND 
                            COALESCE(a.unfiltered_rows, 0) >= 25001
                         THEN 'Dynamic'
                       ELSE 'Static'
                     END, 
                     CASE 
                       WHEN (SELECT compatibility_level 
                               FROM sys.databases 
                              WHERE QUOTENAME(name) = a.DatabaseName) >= 130
                            AND 
                            COALESCE(a.unfiltered_rows, 0) >= 25001
                         THEN CONVERT(BIGINT, SQRT(1000 * COALESCE(a.unfiltered_rows, 0)))
                       ELSE (CASE
				                           WHEN COALESCE(a.unfiltered_rows, 0) IS NULL THEN 0
				                           WHEN COALESCE(a.unfiltered_rows, 0) <= 500 THEN 501
				                           ELSE 500 + CONVERT(BIGINT, COALESCE(a.unfiltered_rows, 0) * 0.2)
			                          END)
                     END) AS Tab3(AutoUpdateThresholdType, AutoUpdateThreshold)
 OUTER APPLY (SELECT b.[Number of rows on table at time statistic was updated],
                     b.[Number of modifications on key column since previous update],
                     b.[Statistic_Updated]
                FROM tempdb.dbo.Tab_GetStatisticInfo_FabianoAmorim b 
               WHERE b.DatabaseName = a.DatabaseName
                 AND b.TableName   = a.TableName
                 AND b.StatsName   = a.StatsName
                 AND b.[Number of statistic data] = 2 /* Previous update stat sample */
                ) AS Tab_StatSample2
 OUTER APPLY (SELECT b.[Number of rows on table at time statistic was updated],
                     b.[Number of modifications on key column since previous update],
                     b.[Statistic_Updated]
                FROM tempdb.dbo.Tab_GetStatisticInfo_FabianoAmorim b 
               WHERE b.DatabaseName = a.DatabaseName
                 AND b.TableName   = a.TableName
                 AND b.StatsName   = a.StatsName
                 AND b.[Number of statistic data] = 3 /* Previous update stat sample */
                ) AS Tab_StatSample3
 OUTER APPLY (SELECT b.[Number of rows on table at time statistic was updated],
                     b.[Number of modifications on key column since previous update],
                     b.[Statistic_Updated]
                FROM tempdb.dbo.Tab_GetStatisticInfo_FabianoAmorim b 
               WHERE b.DatabaseName = a.DatabaseName
                 AND b.TableName   = a.TableName
                 AND b.StatsName   = a.StatsName
                 AND b.[Number of statistic data] = 4 /* Previous update stat sample */
                ) AS Tab_StatSample4
 CROSS APPLY (SELECT SUM(Col1) FROM (VALUES(DATEDIFF(MINUTE, Tab_StatSample2.Statistic_Updated, a.Statistic_Updated)), 
                                           (DATEDIFF(MINUTE, Tab_StatSample3.Statistic_Updated, Tab_StatSample2.Statistic_Updated)), 
                                           (DATEDIFF(MINUTE, Tab_StatSample4.Statistic_Updated, Tab_StatSample3.Statistic_Updated))
                                ) AS Tab(Col1)) AS Tab_MinBetUpdateStats([Tot minutes between update stats])
 CROSS APPLY (SELECT SUM(Col1) FROM (VALUES(a.[Number of modifications on key column since previous update]), 
                                           (Tab_StatSample2.[Number of modifications on key column since previous update]), 
                                           (Tab_StatSample3.[Number of modifications on key column since previous update])
                                ) AS Tab(Col1)) AS Tab_TotModifications([Tot modifications between update stats])
 CROSS APPLY (SELECT CONVERT(NUMERIC(18, 2), Tab_TotModifications.[Tot modifications between update stats] 
                     / CASE 
                         WHEN Tab_MinBetUpdateStats.[Tot minutes between update stats] = 0 THEN 1 
                         ELSE Tab_MinBetUpdateStats.[Tot minutes between update stats] 
                       END)) AS TabModificationsPerMinute([Avg modifications per minute based on existing update stats intervals])
 CROSS APPLY (SELECT DATEDIFF(MINUTE, GETDATE(), CASE 
                                                   WHEN TabModificationsPerMinute.[Avg modifications per minute based on existing update stats intervals] > 0 THEN
				                                               DATEADD(MINUTE, ((Tab3.AutoUpdateThreshold - a.[Current number of modified rows since last update]) / TabModificationsPerMinute.[Avg modifications per minute based on existing update stats intervals]), GETDATE())
			                                                ELSE NULL
		                                               END)) AS TabEstimatedMinsUntilNextUpdateStats([Estimated minutes until next auto update stats])
LEFT OUTER JOIN sys.dm_db_index_usage_stats
ON a.DatabaseID = dm_db_index_usage_stats.database_id
AND a.ObjectID = dm_db_index_usage_stats.object_id
AND (dm_db_index_usage_stats.index_id = CASE WHEN a.[statistic type] = 'Index_Statistic' THEN a.stats_id ELSE 1 END)
OUTER APPLY (SELECT MIN(Dt) FROM (VALUES(dm_db_index_usage_stats.last_user_seek), 
                                        (dm_db_index_usage_stats.last_user_scan), 
                                        (dm_db_index_usage_stats.last_user_lookup)
                               ) AS t(Dt)) AS TabIndexUsage([Last time index(or a table if obj is not a Index_Statistic) was used])
 WHERE a.[Number of statistic data] = 1
   AND a.[Current number of rows on table] > 0 /* Ignoring empty tables */
   AND TabModificationsPerMinute.[Avg modifications per minute based on existing update stats intervals] > 0
ORDER BY [Current number of rows on table] DESC, 
         DatabaseName,
         TableName,
         KeyColumnName,
         StatsName
GO

/* 
  Check 23 - What is the average of minutes it take to update the statistic?

  Statistics being updated too often indicates high number of modifications or
  unecessary statistic updates
*/

;WITH CTE_1
AS
(
SELECT a.DatabaseName,
       a.TableName,
       a.StatsName,
       a.KeyColumnName,
       a.[Current number of rows on table],
       a.Statistic_Updated AS [Update stat - 1 (most recent)],
       Tab_StatSample2.Statistic_Updated AS [Update stat - 2],
       Tab_StatSample3.Statistic_Updated AS [Update stat - 3],
       Tab_StatSample4.Statistic_Updated AS [Update stat - 4],
       a.[Number of modifications on key column since previous update] AS [Update stat - 1, Number of modifications on key column since previous update],
       Tab_StatSample2.[Number of modifications on key column since previous update] AS [Update stat - 2, Number of modifications on key column since previous update],
       Tab_StatSample3.[Number of modifications on key column since previous update] AS [Update stat - 3, Number of modifications on key column since previous update],
       DATEDIFF(MINUTE, Tab_StatSample2.Statistic_Updated, a.Statistic_Updated) AS [Minutes between update stats 1 and 2],
       DATEDIFF(MINUTE, Tab_StatSample3.Statistic_Updated, Tab_StatSample2.Statistic_Updated) AS [Minutes between update stats 2 and 3],
       DATEDIFF(MINUTE, Tab_StatSample4.Statistic_Updated, Tab_StatSample3.Statistic_Updated) AS [Minutes between update stats 3 and 4],
       (SELECT AVG(Col1) FROM (VALUES(DATEDIFF(MINUTE, Tab_StatSample2.Statistic_Updated, a.Statistic_Updated)), 
                                     (DATEDIFF(MINUTE, Tab_StatSample3.Statistic_Updated, Tab_StatSample2.Statistic_Updated)), 
                                     (DATEDIFF(MINUTE, Tab_StatSample4.Statistic_Updated, Tab_StatSample3.Statistic_Updated))
                               ) AS T(Col1)) AS [Avg minutes between update stats]
  FROM tempdb.dbo.Tab_GetStatisticInfo_FabianoAmorim a
 OUTER APPLY (SELECT b.[Number of rows on table at time statistic was updated],
                     b.[Number of modifications on key column since previous update],
                     b.[Statistic_Updated]
                FROM tempdb.dbo.Tab_GetStatisticInfo_FabianoAmorim b 
               WHERE b.DatabaseName = a.DatabaseName
                 AND b.TableName   = a.TableName
                 AND b.StatsName   = a.StatsName
                 AND b.[Number of statistic data] = 2 /* Previous update stat sample */
                ) AS Tab_StatSample2
 OUTER APPLY (SELECT b.[Number of rows on table at time statistic was updated],
                     b.[Number of modifications on key column since previous update],
                     b.[Statistic_Updated]
                FROM tempdb.dbo.Tab_GetStatisticInfo_FabianoAmorim b 
               WHERE b.DatabaseName = a.DatabaseName
                 AND b.TableName   = a.TableName
                 AND b.StatsName   = a.StatsName
                 AND b.[Number of statistic data] = 3 /* Previous update stat sample */
                ) AS Tab_StatSample3
 OUTER APPLY (SELECT b.[Number of rows on table at time statistic was updated],
                     b.[Number of modifications on key column since previous update],
                     b.[Statistic_Updated]
                FROM tempdb.dbo.Tab_GetStatisticInfo_FabianoAmorim b 
               WHERE b.DatabaseName = a.DatabaseName
                 AND b.TableName   = a.TableName
                 AND b.StatsName   = a.StatsName
                 AND b.[Number of statistic data] = 4 /* Previous update stat sample */
                ) AS Tab_StatSample4
WHERE a.[Current number of rows on table] > 0 /* Ignoring empty tables */
  AND a.[Number of statistic data] = 1
)
SELECT 'Check 23 - What is the average of minutes it take to update the statistic?' AS [Info],
       CTE_1.DatabaseName,
       CTE_1.TableName,
       CTE_1.StatsName,
       CTE_1.KeyColumnName,
       CTE_1.[Current number of rows on table],
       CTE_1.[Avg minutes between update stats],
       'Statistic is updated every ' 
       + CONVERT(VarChar(4), DATEDIFF(mi, '19000101', (DATEADD(mi, CTE_1.[Avg minutes between update stats], '19000101'))) / 60 / 24) + 'd '
       + CONVERT(VarChar(4), DATEDIFF(mi, '19000101', (DATEADD(mi, CTE_1.[Avg minutes between update stats], '19000101'))) / 60 % 24) + 'hr '
       + CONVERT(VarChar(4), DATEDIFF(mi, '19000101', (DATEADD(mi, CTE_1.[Avg minutes between update stats], '19000101'))) % 60) + 'min' AS [Comment 0],
       CTE_1.[Update stat - 1 (most recent)],
       CTE_1.[Update stat - 2],
       CTE_1.[Update stat - 3],
       CTE_1.[Update stat - 4]
  FROM CTE_1
  WHERE 1=1
ORDER BY [Current number of rows on table] DESC, 
         DatabaseName,
         TableName,
         KeyColumnName,
         StatsName       
GO


/*
  Check 24 - Check if statistic percent sample is too small

  When Microsoft SQL Server creates or updates statistics,
  if a sampling rate isn't manually specified, SQL Server will 
  calculate a default sampling rate. Depending on the real distribution 
  of data in the underlying table, the default sampling rate may 
  not accurately represent the data distribution. 
  This may cause degradation of query plan efficiency.

  To improve this scenario, a database administrator can choose to manually 
  update statistics by using a fixed (fullscan? anyone?) sampling rate 
  that can better represent the distribution of data.
*/

SELECT 'Check 24 - Check if statistic percent sample is too small' AS [Info],
       a.DatabaseName,
       a.TableName,
       a.StatsName,
       a.KeyColumnName,
       a.Statistic_Updated,
       a.[Current number of rows on table],
       a.[Number of rows sampled on last update/create statistic],
       a.[Percent sampled],
       a.[Percent sample comment]
  FROM tempdb.dbo.Tab_GetStatisticInfo_FabianoAmorim a
WHERE a.[Current number of rows on table] > 0 /* Ignoring empty tables */
  AND a.[Number of statistic data] = 1
ORDER BY [Current number of rows on table] DESC, 
         DatabaseName,
         TableName,
         KeyColumnName,
         StatsName
GO

/*
  Check 25 - Check if if maybe good to adjust MAXDOP on UPDATE STATISTIC command

  If table is too big (over a million rows), it may be a good idea to specify MAXDOP to increase 
  number of CPUs available on update stats command.
  Default is to use whatever is specified on MAXDOP at the instance level. 

  Note: MAXDOP option is only available on SQL Server 2014 (SP3), 2016 (SP2), 2017 (CU3) and higher builds.
  https://support.microsoft.com/en-us/topic/kb4041809-update-adds-support-for-maxdop-option-for-create-statistics-and-update-statistics-statements-in-sql-server-2014-2016-and-2017-62da8a67-3461-3ec2-ae72-df10a464a209

  Check if MAXDOP at instance is lower than available CPUs and recommend to increate MAXDOP.
*/

DECLARE @cpucount INT, @maxdop INT
	SELECT @cpucount = COUNT(cpu_id)
	FROM sys.dm_os_schedulers
	WHERE scheduler_id < 255 AND parent_node_id < 64

SELECT @maxdop = CONVERT(INT, value)
FROM sys.configurations
WHERE name = 'max degree of parallelism';

DECLARE @MaxNumberofRows BIGINT
SELECT 'Check 25 - Check if statistic percent sample is too small' AS [Info],
       DatabaseName, 
       MAX([Current number of rows on table]) AS [Max number of rows in a table],
       COUNT(CASE WHEN [Current number of rows on table] >= 1000000 /*1mi*/ THEN 1 ELSE NULL END) AS [Number of tables with more than 1mi rows],
       CASE 
         WHEN MAX([Current number of rows on table]) >= 1000000 /*1mi*/
          AND @maxdop < @cpucount
         THEN 'Warning - Database ' + DatabaseName + 
              ' has ' + 
              CONVERT(VarChar, COUNT(CASE WHEN [Current number of rows on table] >= 1000000 /*1mi*/ THEN 1 ELSE NULL END)) + 
              ' tables with more than 1mi rows. Update stats is currently running with MAXDOP of ' +
              CONVERT(VarChar, @maxdop) + 
              ' and there are ' +
              CONVERT(VarChar, @cpucount) + 
              ' CPUs available.' +  
              ' Consider to increase MAXDOP on UPDATE STATISTICS command to speed up the update at cost of use more CPU.'
         ELSE 'OK'
       END AS [Comment]
  FROM tempdb.dbo.Tab_GetStatisticInfo_FabianoAmorim a
GROUP BY DatabaseName
GO


/*
  Check 26 - Check if statistic is set to no_recompute
*/

SELECT 'Check 26 - Check if statistic is set to no_recompute' AS [Info],
       a.DatabaseName,
       a.TableName,
       a.StatsName,
       a.KeyColumnName,
       a.Statistic_Updated,
       a.[Current number of rows on table],
       a.[Number of rows sampled on last update/create statistic],
       a.no_recompute,
       a.[No recompute comment]
  FROM tempdb.dbo.Tab_GetStatisticInfo_FabianoAmorim a
WHERE a.[Current number of rows on table] > 0 /* Ignoring empty tables */
  AND a.[Number of statistic data] = 1
ORDER BY a.no_recompute DESC,
         [Current number of rows on table] DESC, 
         DatabaseName,
         TableName,
         KeyColumnName,
         StatsName
GO


/*
  Check 27 - Check statistic key column with large value types.

  Statsistic creation/update on LOB columns may take a lot of time to run.
*/

SELECT 'Check 27 - Check statistic key column with large value types.' AS [Info],
       a.DatabaseName,
       a.TableName,
       a.StatsName,
       a.KeyColumnName,
       a.Statistic_Updated,
       a.[Current number of rows on table],
       a.[Number of rows sampled on last update/create statistic],
       a.IsLOB,
       a.[Statistic on large value type comment]
  FROM tempdb.dbo.Tab_GetStatisticInfo_FabianoAmorim a
WHERE a.[Current number of rows on table] > 0 /* Ignoring empty tables */
  AND a.[Number of statistic data] = 1
ORDER BY a.IsLOB DESC, 
         [Current number of rows on table] DESC, 
         DatabaseName,
         TableName,
         KeyColumnName,
         StatsName
GO

/*
  Check 28 - Check if there are tables with more statistics than columns
*/

SELECT 'Check 28 - Check if there are tables with more statistics than columns' AS [Info],
       a.DatabaseName,
       a.TableName,
       a.StatsName,
       a.KeyColumnName,
       a.Statistic_Updated,
       a.[Current number of rows on table],
       a.[Number of rows sampled on last update/create statistic],
       a.[Number of statistics in this table],
       a.[Number of statistics comment]
  FROM tempdb.dbo.Tab_GetStatisticInfo_FabianoAmorim a
WHERE a.[Current number of rows on table] > 0 /* Ignoring empty tables */
  AND a.[Number of statistic data] = 1
ORDER BY [Number of statistics in this table] DESC, 
         DatabaseName,
         TableName,
         KeyColumnName,
         StatsName
GO

/*
  Check 29 - Check if there are duplicated statistics

  Statistic is considered duplicated has it already has another Index_Statistic on 
  the same key (or keys as long as leading is the same) column(s) and filter_definition.
*/

SELECT 'Check 29 - Check if there are duplicated statistics' AS [Info],
       a.DatabaseName,
       a.TableName,
       a.StatsName,
       a.KeyColumnName,
       a.StatAllColumns,
       a.[Statistic type],
       a.filter_definition,
       a.Statistic_Updated,
       a.[Current number of rows on table],
       a.[Number of rows sampled on last update/create statistic],
       a.[Auto created stats duplicated comment],
       CASE 
         WHEN a.[Auto created stats duplicated comment] <> 'OK' THEN Drop_Stat_Command
         ELSE NULL
       END AS [Drop stat command]
  FROM tempdb.dbo.Tab_GetStatisticInfo_FabianoAmorim a
WHERE a.[Current number of rows on table] > 0 /* Ignoring empty tables */
  AND a.[Number of statistic data] = 1
  AND a.[Auto created stats duplicated comment] <> 'OK'
ORDER BY a.[Current number of rows on table] DESC, 
         DatabaseName,
         TableName,
         KeyColumnName,
         StatsName
GO


/*
  Check 30 - Check if there are tables with more than 10mi rows
*/

SELECT 'Check 30 - Check if there are tables with more than 10mi rows' AS [Info],
       a.DatabaseName,
       a.TableName,
       a.StatsName,
       a.KeyColumnName,
       a.[Statistic type],
       a.Statistic_Updated,
       a.[Current number of rows on table],
       a.[Number of rows sampled on last update/create statistic],
       a.[Percent sampled],
       [Number of rows comment]
  FROM tempdb.dbo.Tab_GetStatisticInfo_FabianoAmorim a
WHERE a.[Current number of rows on table] > 0 /* Ignoring empty tables */
  AND a.[Number of statistic data] = 1
ORDER BY a.[Current number of rows on table] DESC, 
         DatabaseName,
         TableName,
         KeyColumnName,
         StatsName
GO

/*
  Check 31 - Check if there are outdated (latest update older thhan 24 hours) statistics
*/

SELECT 
  'Check 31 - Check if there are outdated (latest update older thhan 24 hours) statistics' AS [Info],
  [Number of statistic data],  
  TableName, 
  StatsName, 
  KeyColumnName, 
  [Statistic type],
  [Statistic_Updated],
  DATEDIFF(hh,[Statistic_Updated],GETDATE()) AS [Hours since last update],
  CASE 
    WHEN DATEDIFF(hh,[Statistic_Updated], GETDATE()) > 24 THEN 
         'It has been more than 24 hours [' + CONVERT(VarChar(4), DATEDIFF(mi,[Statistic_Updated],GETDATE()) / 60 / 24) + 'd ' + CONVERT(VarChar(4), DATEDIFF(mi,[Statistic_Updated],GETDATE()) / 60 % 24) + 'hr '
         + CONVERT(VarChar(4), DATEDIFF(mi,[Statistic_Updated],GETDATE()) % 60) + 'min' 
         + '] since last update statistic.'
    ELSE 'OK'
  END AS [Comment 1],
  [Modification comment 1] AS [Comment 2],
  TabIndexUsage.[Last time index(or a table if obj is not a Index_Statistic) was used],
  [Current number of rows on table],
  [Number of rows on table at time statistic was updated],
  [Current number of modified rows since last update],
  a.user_seeks + a.user_scans + a.user_lookups AS [Number of reads on index/table since last restart],
  a.user_updates AS [Number of modifications on index/table since last restart],
  a.range_scan_count AS [Number of range scans since last restart/rebuild],
  a.page_latch_wait_count AS [Number of page latch since last restart/rebuild],
  a.page_io_latch_wait_count AS [Number of page I/O latch since last restart/rebuild],
  Tab3.AutoUpdateThreshold,
  Tab3.AutoUpdateThresholdType,
  CONVERT(DECIMAL(18, 2), (a.[Current number of modified rows since last update] / (Tab3.AutoUpdateThreshold * 1.0)) * 100.0) AS [Percent of threshold]
FROM tempdb.dbo.Tab_GetStatisticInfo_FabianoAmorim AS a
LEFT OUTER JOIN sys.dm_db_index_usage_stats
ON a.DatabaseID = dm_db_index_usage_stats.database_id
AND a.ObjectID = dm_db_index_usage_stats.object_id
AND (dm_db_index_usage_stats.index_id = CASE WHEN a.[statistic type] = 'Index_Statistic' THEN a.stats_id ELSE 1 END)
OUTER APPLY (SELECT MIN(Dt) FROM (VALUES(dm_db_index_usage_stats.last_user_seek), 
                                        (dm_db_index_usage_stats.last_user_scan)
                               ) AS t(Dt)) AS TabIndexUsage([Last time index(or a table if obj is not a Index_Statistic) was used])
CROSS APPLY (SELECT CASE 
                       WHEN (SELECT compatibility_level 
                               FROM sys.databases 
                              WHERE QUOTENAME(name) = a.DatabaseName) >= 130 
                            AND 
                            COALESCE(a.unfiltered_rows, 0) >= 25001
                         THEN 'Dynamic'
                       ELSE 'Static'
                     END, 
                     CASE 
                       WHEN (SELECT compatibility_level 
                               FROM sys.databases 
                              WHERE QUOTENAME(name) = a.DatabaseName) >= 130
                            AND 
                            COALESCE(a.unfiltered_rows, 0) >= 25001
                         THEN CONVERT(BIGINT, SQRT(1000 * COALESCE(a.unfiltered_rows, 0)))
                       ELSE (CASE
				                           WHEN COALESCE(a.unfiltered_rows, 0) IS NULL THEN 0
				                           WHEN COALESCE(a.unfiltered_rows, 0) <= 500 THEN 501
				                           ELSE 500 + CONVERT(BIGINT, COALESCE(a.unfiltered_rows, 0) * 0.2)
			                          END)
                     END) AS Tab3(AutoUpdateThresholdType, AutoUpdateThreshold)
WHERE [Number of statistic data] = 1
ORDER BY [Current number of rows on table] DESC, 
         DatabaseName,
         TableName,
         KeyColumnName,
         StatsName
GO


/*
  Check 32 - Check if there are empty histograms
*/

SELECT 'Check 32 - Check if there are empty histograms' AS [Info],
       a.DatabaseName,
       a.TableName,
       a.StatsName,
       a.KeyColumnName,
       a.[Statistic type],
       a.Statistic_Updated,
       a.[Current number of rows on table],
       a.[Number of rows sampled on last update/create statistic],
       a.[Percent sampled],
       a.[Number of steps on histogram],
       a.[Number of steps comment]
  FROM tempdb.dbo.Tab_GetStatisticInfo_FabianoAmorim a
WHERE a.[Current number of rows on table] > 0 /* Ignoring empty tables */
  AND a.[Number of statistic data] = 1
  AND a.[Number of steps comment] <> 'OK'
ORDER BY a.[Number of steps on histogram] ASC, 
         DatabaseName,
         TableName,
         KeyColumnName,
         StatsName
GO


/*
  Check 33 - Check if there are tables with more than 10mi rows and need to do a parallel update stats with TF7471
  
  The idea is to reduce update stats mainntenance time duration by adding a parallel statistic maintenance plan 
  that runs multiple UPDATE STATISTICS for different statistics on a single table concurrently.

  We can easily leaverage of service broker and Ola's maintenance script to do it.
*/

SELECT 'Check 33 - Check if there are tables with more than 10mi rows and need to do a parallel update stats with TF7471' AS [Info],
       DatabaseName, 
       MAX([Current number of rows on table]) AS [Max number of rows in a table],
       COUNT(CASE WHEN [Current number of rows on table] >= 10000000 /*10mi*/ THEN 1 ELSE NULL END) AS [Number of tables with more than 10mi rows],
       CASE 
         WHEN MAX([Current number of rows on table]) >= 10000000 /*10mi*/
         THEN 'Warning - Database ' + DatabaseName + 
              ' has ' + 
              CONVERT(VarChar, COUNT(CASE WHEN [Current number of rows on table] >= 10000000 /*10mi*/ THEN 1 ELSE NULL END)) + 
              ' tables with more than 10mi rows. Consider to create a maintenance plan to run update stats in parallel using Service Broker and TF7471.'
         ELSE 'OK'
       END AS [Comment]
  FROM tempdb.dbo.Tab_GetStatisticInfo_FabianoAmorim a
GROUP BY DatabaseName
GO

/*
  Check 34 - Check if PERSIST_SAMPLE_PERCENT is being used.
  
  PERSIST_SAMPLE_PERCENT option of 
  set and retain a specific sampling percentage for subsequent statistic updates 
  that do not explicitly specify a sampling percentage.

  That means an auto-update stat will retain value specified on PERSIST_SAMPLE_PERCENT.
  If an user set it to 100, an auto-update stats may trigger an unexpected update
  using fullscan, which depending on the table size, can take A LOT of time to run.

  Note: PERSIST_SAMPLE_PERCENT is only availabe on SQL Server 2016 SP1 CU4, 2017 CU1, and higher builds.
*/

DECLARE @sqlmajorver INT, @sqlminorver int, @sqlbuild int
SELECT @sqlmajorver = CONVERT(int, (@@microsoftversion / 0x1000000) & 0xff),
	      @sqlminorver = CONVERT(int, (@@microsoftversion / 0x10000) & 0xff),
 	     @sqlbuild = CONVERT(int, @@microsoftversion & 0xffff);

IF (@sqlmajorver = 13 /*SQL2016*/ AND @sqlbuild >= 4446 /*SP1 CU4*/)
   OR
   (@sqlmajorver = 14 /*SQL2017*/ AND @sqlbuild >= 3006 /*CU1*/)
   OR
   (@sqlmajorver >= 15 /*SQL2019*/)
BEGIN
  IF OBJECT_ID('tempdb.dbo.#db') IS NOT NULL
    DROP TABLE #db

  IF OBJECT_ID('tempdb.dbo.#tmp_StatsUsingPersistedSample') IS NOT NULL
    DROP TABLE #tmp_StatsUsingPersistedSample

  CREATE TABLE #tmp_StatsUsingPersistedSample (DatabaseID Int,
                                               ObjectID   Int,
                                               Stats_ID    INT,
                                               has_persisted_sample BIT)

  SELECT d1.[name] INTO #db
  FROM sys.databases d1
  where d1.state_desc = 'ONLINE' and is_read_only = 0
  and d1.database_id in (SELECT DISTINCT DatabaseID FROM tempdb.dbo.Tab_GetStatisticInfo_FabianoAmorim)

  DECLARE @SQL VarChar(MAX)
  declare @database_name sysname
  DECLARE @ErrMsg VarChar(8000)

  DECLARE c_databases CURSOR read_only FOR
      SELECT [name] FROM #db
  OPEN c_databases

  FETCH NEXT FROM c_databases
  into @database_name
  WHILE @@FETCH_STATUS = 0
  BEGIN
    SET @ErrMsg = 'Checking incremental stats on DB - [' + @database_name + ']'
    RAISERROR (@ErrMsg, 10, 1) WITH NOWAIT

    SET @SQL = 'use [' + @database_name + ']; 
                SELECT t.DatabaseID, 
                       t.ObjectID, 
                       t.Stats_ID,
                       stats.has_persisted_sample
                FROM tempdb.dbo.Tab_GetStatisticInfo_FabianoAmorim AS t
                INNER JOIN sys.stats
                ON stats.OBJECT_ID = t.ObjectID
                AND stats.stats_id = t.Stats_ID
                WHERE [Number of statistic data] = 1
                AND DatabaseName = QUOTENAME(DB_NAME())'

    /*SELECT @SQL*/
    INSERT INTO #tmp_StatsUsingPersistedSample
    EXEC (@SQL)
  
    FETCH NEXT FROM c_databases
    into @database_name
  END
  CLOSE c_databases
  DEALLOCATE c_databases

  SELECT 'Check 34 - Check if PERSIST_SAMPLE_PERCENT is being used.' AS [Info],
         a.DatabaseName,
         a.TableName,
         a.StatsName,
         a.KeyColumnName,
         a.[Statistic type],
         a.[Current number of rows on table],
         a.[Number of rows sampled on last update/create statistic],
         a.[Percent sampled],
         #tmp_StatsUsingPersistedSample.has_persisted_sample,
         CASE
           WHEN (a.[Percent sampled] = 100)
            AND (a.is_auto_update_stats_on = 1) 
            AND (#tmp_StatsUsingPersistedSample.has_persisted_sample = 0)
           THEN 'Warning - Last update stat was done executed using FULLSCAN without has_persisted_sample. Since auto update stats is ON for this DB, if an auto update stats runs, it will reset stat back to the default sampling rate, and possibly introduce degradation of query plan efficiency.'
           WHEN (a.[Percent sampled] = 100)
            AND (a.is_auto_update_stats_on = 1)
            AND (#tmp_StatsUsingPersistedSample.has_persisted_sample = 1)
           THEN 'Warning - Statistic is set to use persisted sample and last sample was 100% (FULLSCAN). An auto-update stats may trigger an unexpected update using fullscan, which can take A LOT of time to run and will increase query plan compilation time.'
           WHEN (#tmp_StatsUsingPersistedSample.has_persisted_sample = 1)
           THEN 'Information - Statistic is set to use persisted sample. Please review the last update percent sample and make sure this is using an expected value. Update stats with large samples may take a lof ot time to run.'
           ELSE 'OK'
         END AS [Comment],
         CASE
           WHEN #tmp_StatsUsingPersistedSample.has_persisted_sample = 0
           THEN 'UPDATE STATISTICS ' + a.DatabaseName + '.' + a.SchemaName + '.' + a.TableName + ' ' + a.StatsName + 
                ' WITH PERSIST_SAMPLE_PERCENT = ON, /*FULLSCAN*/ /*SAMPLE <n> PERCENT*/;'
           ELSE NULL
         END AS [Command to implement persisted sample]
  FROM tempdb.dbo.Tab_GetStatisticInfo_FabianoAmorim AS a
  INNER JOIN #tmp_StatsUsingPersistedSample
  ON #tmp_StatsUsingPersistedSample.DatabaseID = a.DatabaseID
  AND #tmp_StatsUsingPersistedSample.ObjectID = a.ObjectID
  AND #tmp_StatsUsingPersistedSample.Stats_ID = a.Stats_ID
  WHERE [Number of statistic data] = 1
  ORDER BY a.[Current number of rows on table] DESC, 
           a.DatabaseName,
           a.TableName,
           a.KeyColumnName,
           a.StatsName
END
ELSE
BEGIN
  SELECT 'Check 34 - Check if PERSIST_SAMPLE_PERCENT is being used.' AS [Info], 
         'Check is not relevant on this SQL version as PERSIST_SAMPLE_PERCENT PERSIST_SAMPLE_PERCENT is only availabe on SQL Server 2016 SP1 CU4, 2017 CU1, and higher builds.' AS [Auto create stats incremental comment]
END
GO

/*
  Check 35 - Check if there was an event of an auto update stat using a sample smaller than the last sample used
  
  A DBA can choose to manually update statistics by using a fixed sampling rate 
  that can better represent the distribution of data.
  However, a subsequent Automatic Update Statistics operation will reset back to the default 
  sampling rate, and possibly introduce degradation of query plan efficiency.
  
  In this check I'm returning all stats that have a diff in the number of steps, 
  make sure you review all of those (not only the ones with a warning) to confirm you
  identified all the cases.

  Ideally, this check should be executed after the update stat maintenance, 
  the longer the diff after the maintenance the better the chances we capture
  histogram diff due to an auto update stat.
  For instance, if the maintenance plan runs at 12AM, it would be nice to run this at 
  5PM to see if there was any auto update that caused histogram change during the day.

  PERSIST_SAMPLE_PERCENT command can be used to avoid this issue.
  Starting with SQL Server 2016 (13.x) SP1 CU4, use the PERSIST_SAMPLE_PERCENT option of 
  CREATE STATISTICS or UPDATE STATISTICS, to set and retain a specific sampling percentage 
  for subsequent statistic updates that do not explicitly specify a sampling percentage.

  Note: Only availabe on SQL Server 2016 SP1 CU4 and 2017 CU1, and higher builds.

  Another option is to add a job to manually update the stats more frequently, or to recreate the 
  stats with NO_RECOMPUTE and make sure you have your own job taking care of it.
*/



SELECT 'Check 35 - Check if there was an event of an auto update stat using a sample smaller than the last sample used' AS [Info],
       a.DatabaseName,
       a.TableName,
       a.StatsName,
       a.KeyColumnName,
       a.[Statistic type],
       a.[Current number of rows on table],
       a.[Number of modifications on key column since previous update],
       Tab3.AutoUpdateThreshold,
       Tab3.AutoUpdateThresholdType,
       CONVERT(DECIMAL(18, 2), (a.[Current number of modified rows since last update] / (Tab3.AutoUpdateThreshold * 1.0)) * 100.0) AS [Percent of threshold],
       a.[Number of rows on table at time statistic was updated] AS [Number of rows on table at time statistic was updated 1 - most recent],
       Tab_StatSample2.[Number of rows on table at time statistic was updated] AS  [Number of rows on table at time statistic was updated - 2 - previous update],
       a.[Number of rows sampled on last update/create statistic],
       a.[Percent sampled],
       a.[Number of steps on histogram] AS [Number of steps on histogram 1 - most recent],
       Tab_StatSample2.[Number of steps on histogram] AS [Number of steps on histogram 2 - previous update],
       a.Statistic_Updated AS [Update stat 1 - most recent],
       Tab_StatSample2.Statistic_Updated AS [Update stat 2 - previous update],
       steps_diff_pct,
       CASE
         WHEN 
          (steps_diff_pct < 90) 
          /*Only considering stats where number of steps diff is at least 90%*/
          AND (a.[Number of modifications on key column since previous update] < 1000000) 
          /*Checking if number of modifications is lower than 1mi, because, if number of modifications
            is greater than 1mi, it may be the reason of why number of steps changed.
            If number of modifications is low and steps is diff, then it is very likely it changed because
            of an update with a lower sample*/
         THEN 'Warning - Number of steps on last update stats is greater than the last update stats. This may indicate that stat was updated with a lower sample.'
         ELSE 'OK'
       END AS [Comment]
FROM tempdb.dbo.Tab_GetStatisticInfo_FabianoAmorim AS a
CROSS APPLY (SELECT b.[Number of rows on table at time statistic was updated],
                    b.[Statistic_Updated],
                    b.[Number of steps on histogram]
                FROM tempdb.dbo.Tab_GetStatisticInfo_FabianoAmorim b 
               WHERE b.DatabaseName = a.DatabaseName
                 AND b.TableName   = a.TableName
                 AND b.StatsName   = a.StatsName
                 AND b.[Number of statistic data] = 2 /* Previous update stat sample */
                ) AS Tab_StatSample2
CROSS APPLY (SELECT CAST((a.[Number of steps on histogram] / (Tab_StatSample2.[Number of steps on histogram] * 1.00)) * 100.0 AS DECIMAL(18, 2))) AS t(steps_diff_pct)
CROSS APPLY (SELECT CASE 
                       WHEN (SELECT compatibility_level 
                               FROM sys.databases 
                              WHERE QUOTENAME(name) = a.DatabaseName) >= 130 
                            AND 
                            COALESCE(a.unfiltered_rows, 0) >= 25001
                         THEN 'Dynamic'
                       ELSE 'Static'
                     END, 
                     CASE 
                       WHEN (SELECT compatibility_level 
                               FROM sys.databases 
                              WHERE QUOTENAME(name) = a.DatabaseName) >= 130
                            AND 
                            COALESCE(a.unfiltered_rows, 0) >= 25001
                         THEN CONVERT(BIGINT, SQRT(1000 * COALESCE(a.unfiltered_rows, 0)))
                       ELSE (CASE
				                           WHEN COALESCE(a.unfiltered_rows, 0) IS NULL THEN 0
				                           WHEN COALESCE(a.unfiltered_rows, 0) <= 500 THEN 501
				                           ELSE 500 + CONVERT(BIGINT, COALESCE(a.unfiltered_rows, 0) * 0.2)
			                          END)
                     END) AS Tab3(AutoUpdateThresholdType, AutoUpdateThreshold)
WHERE [Number of statistic data] = 1
AND a.[Percent sampled] <> 100 /*Only considering stats not using FULLSCAN*/
AND Tab_StatSample2.[Number of steps on histogram] <> 1 /*Ignoring histograms with only 1 step*/
AND a.[Number of steps on histogram] <> Tab_StatSample2.[Number of steps on histogram] /*Only cases where number of steps is diff*/
ORDER BY steps_diff_pct ASC, 
         a.[Current number of rows on table] DESC, 
         a.DatabaseName,
         a.TableName,
         a.KeyColumnName,
         a.StatsName
GO

/*
  Check 36 - Check if there are hypothetical statistics created by DTA.

  Hypothetical indexes are created by the Database Tuning Assistant (DTA) during its tests. 
  If a DTA session was interrupted, these indexes may not be deleted. 
  It is recommended to drop these objects as soon as possible.
*/

SELECT 'Check 36 - Check if there are hypothetical statistics created by DTA.' AS [Info],
       a.DatabaseName,
       a.TableName,
       a.StatsName,
       a.KeyColumnName,
       a.[Statistic type],
       a.[Current number of rows on table],
       a.Statistic_Updated,
       t.[Comment],
       CASE 
         WHEN (a.StatsName LIKE '%_dta_stat%')
         THEN Drop_Stat_Command
         ELSE NULL
       END AS [Drop stat command]
FROM tempdb.dbo.Tab_GetStatisticInfo_FabianoAmorim AS a
CROSS APPLY (SELECT CASE
                      WHEN (a.StatsName LIKE '%_dta_stat%')
                      THEN 'Warning - It looks like this is an hypothetical statistic. Hypothetical objects are created by the Database Tuning Assistant (DTA) during its tests. If a DTA session was interrupted, these indexes may not be deleted. It is recommended to drop these objects as soon as possible.'
                      ELSE 'OK'
                    END) AS t([Comment])
WHERE [Number of statistic data] = 1
AND t.Comment <> 'OK'
AND [Statistic type] <> 'Index_Statistic'
ORDER BY [Comment], 
         a.[Current number of rows on table] DESC, 
         a.DatabaseName,
         a.TableName,
         a.KeyColumnName,
         a.StatsName
GO

/*
  Check 37 - Check if Commandlog exists on master and return updatestats command and duration.

  You may want to adjust your maintenance plan to deal with statistics taking too much time to 
  run in a separate window.
  Also, it would be good to know whether the statistic is really being used as you don't want to 
  spend time updating it if it is not helping your queries.
  You can use TF8666 and check plan cache for the stats name to see if you can find any usage from cache.
  But, keep in mind that plan cache may be under pressure or bloated with ad-hoc plans causing plans to
  be removed very quick, so you may don't capture the plan from cache.
  We might think (I wish) that a properly architected database system making extensive use 
  of stored procedures should not have unusually large plan cache. 
  But many real-world systems are not well-architected, either in having 
  too much dynamic SQL, or in the case of Entity Frameworks, a bloated paramterized SQL plan cache.

  Another option is to track it using auto_stats extended event filtering by the stat name to see
  if this is loaded.
*/

IF OBJECT_ID('master.dbo.CommandLog') IS NOT NULL
BEGIN
  DECLARE @SQL VarChar(MAX)
  SET @SQL = 'use [master]; 
              SELECT ''Check 37 - Check if Commandlog exists on master and return updatestats command and duration'' AS [Info],
                     CommandLog.ID,
                     CommandLog.DatabaseName,
                     CommandLog.SchemaName,
                     CommandLog.ObjectName,
                     CommandLog.StatisticsName,
                     a.KeyColumnName,
                     a.[Current number of rows on table],
                     a.[Statistic type],
                     CommandLog.PartitionNumber,
                     DATEDIFF(ms, CommandLog.StartTime, CommandLog.EndTime) AS Duration_Ms,
                     CONVERT(NUMERIC(18, 3), DATEDIFF(ms, CommandLog.StartTime, CommandLog.EndTime) / 1000.) AS Duration_Seconds,
                     CONVERT(NUMERIC(18, 3), DATEDIFF(ms, CommandLog.StartTime, CommandLog.EndTime) / 1000. / 60) AS Duration_Minutes,
                     CASE 
                       WHEN PATINDEX(''%FULLSCAN'', CommandLog.Command) > 0 THEN 1
                       ELSE 0
                     END AS IsFullScan,
                     CommandLog.Command,
                     CommandLog.CommandType,
                     CommandLog.StartTime,
                     CommandLog.EndTime,
                     CommandLog.ErrorNumber,
                     CommandLog.ErrorMessage,
                     CommandLog.ExtendedInfo
               FROM CommandLog
               LEFT OUTER JOIN tempdb.dbo.Tab_GetStatisticInfo_FabianoAmorim AS a
               ON a.DatabaseName = QUOTENAME(CommandLog.DatabaseName)
               AND a.SchemaName = QUOTENAME(CommandLog.SchemaName)
               AND a.TableName = QUOTENAME(CommandLog.ObjectName)
               AND a.StatsName = QUOTENAME(CommandLog.StatisticsName)
               AND a.[Number of statistic data] = 1
               WHERE CommandLog.CommandType = ''UPDATE_STATISTICS''
               ORDER BY DATEDIFF(ms, CommandLog.StartTime, CommandLog.EndTime) DESC'

  /*SELECT @SQL*/
  EXEC (@SQL)
END
GO


/*
  Check 38 - Check if table is partitioned and warn that alter index rebuild will update stats with default sampling rate.

  SQL Server 2012 changed the behavior for partitioned table.
  If a table is partitioned, ALTER INDEX REBUILD will only update statistics 
  for that index with default sampling rate. 
  In other words, it is no longer a FULLSCAN, if you want fullscan, 
  you will need to run UPDATE STATISTCS WITH FULLSCAN.
*/

IF EXISTS(SELECT * FROM tempdb.dbo.Tab_GetStatisticInfo_FabianoAmorim AS a WHERE a.IsTablePartitioned = 1)
BEGIN
  SELECT 
    'Check 38 - Check if table is partitioned and warn that alter index rebuild will update stats with default sampling rate.' AS [Info],
    a.DatabaseName,
    a.TableName,
    a.StatsName,
    a.KeyColumnName,
    a.[Statistic type],
    a.IsTablePartitioned,
    a.Statistic_Updated,
    a.[Current number of rows on table],
    a.[Number of rows sampled on last update/create statistic],
    a.[Percent sampled],
    'Warning - Alter index with rebuild on partitioned tables will use a default sampling rate. If possible, make sure you have a update stats with FULLSCAN.'
  FROM tempdb.dbo.Tab_GetStatisticInfo_FabianoAmorim AS a
  WHERE a.[Number of statistic data] = 1
  AND a.IsTablePartitioned = 1
  ORDER BY a.[Current number of rows on table] DESC, 
           DatabaseName,
           TableName,
           KeyColumnName,
           StatsName
END
ELSE
BEGIN
  SELECT 
    'Check 38 - Check if table is partitioned and warn that alter index rebuild will update stats with default sampling rate.' AS [Info],
    'There are no partitioned tables, check is not relevant.' AS [Comment]
END
GO

/*
  Check 39 - Check if there are "anti-matter" columns, as this may cause issues with update stats.
  
  During the build phase, rows in the new "in-build" index may be in an intermediate state called antimatter. 
  This mechanism allows concurrent DELETE statements to leave a trace for the index builder transaction 
  to avoid inserting deleted rows. At the end of the index build operation 
  all antimatter rows should be cleared. If an error occurs and antimatter rows remain in the index.
  Rebuilding the index will remove the antimatter rows and resolve the error.

  I had weird issues when table had "anti-matter" columns and update stats were not saved... 
  I didn't had time to create a repro, but, I'll do it later. For now I'll check it
  and recommend the rebuild to avoid issues.
*/

IF OBJECT_ID('tempdb.dbo.#db') IS NOT NULL
  DROP TABLE #db

IF OBJECT_ID('tempdb.dbo.#tmp_AntiMatterColumns') IS NOT NULL
  DROP TABLE #tmp_AntiMatterColumns

CREATE TABLE #tmp_AntiMatterColumns (DatabaseName VARCHAR(400),
                                     TableName VARCHAR(400),
                                     IndexName VARCHAR(400),
                                     number_of_rows BigInt,
                                     Command VARCHAR(MAX))

SELECT d1.[name] INTO #db
FROM sys.databases d1
where d1.state_desc = 'ONLINE' and is_read_only = 0
and d1.database_id in (SELECT DISTINCT DatabaseID FROM tempdb.dbo.Tab_GetStatisticInfo_FabianoAmorim)

DECLARE @SQL VarChar(MAX)
declare @database_name sysname
DECLARE @ErrMsg VarChar(8000)

DECLARE c_databases CURSOR read_only FOR
    SELECT [name] FROM #db
OPEN c_databases

FETCH NEXT FROM c_databases
into @database_name
WHILE @@FETCH_STATUS = 0
BEGIN
  SET @ErrMsg = 'Checking anti-matter columns on DB - [' + @database_name + ']'
  RAISERROR (@ErrMsg, 10, 1) WITH NOWAIT

  SET @SQL = 'use [' + @database_name + ']; 
              SELECT DISTINCT 
                     DB_NAME() AS DatabaseName, 
                     OBJECT_NAME(p.object_id) AS TableName,
                     i.name AS IndexName,
                     t.number_of_rows,
                     ''ALTER INDEX '' + QUOTENAME(i.name) + 
                     + '' ON '' + QUOTENAME(DB_NAME()) + ''.'' + 
                                QUOTENAME(OBJECT_SCHEMA_NAME(p.object_id)) + ''.'' +
                                QUOTENAME(OBJECT_NAME(p.object_id)) + 
                     '' REBUILD WITH('' + 
                     CASE 
                       WHEN CONVERT(VarChar(200), SERVERPROPERTY(''Edition'')) LIKE ''Developer%''
                         OR CONVERT(VarChar(200), SERVERPROPERTY(''Edition'')) LIKE ''Enterprise%'' THEN ''ONLINE=ON)''
                       ELSE ''ONLINE=OFF)''
                     END AS [Command]
              FROM sys.system_internals_partitions p
              INNER JOIN sys.system_internals_partition_columns pc
	              ON p.partition_id = pc.partition_id
              LEFT OUTER JOIN sys.indexes i
              ON p.object_id = i.object_id
              AND p.index_id = i.index_id
              OUTER APPLY (SELECT partitions.rows
                           FROM sys.partitions
                           WHERE p.object_id = partitions.object_id
                           AND partitions.index_id <= 1
                           AND partitions.partition_number <= 1) AS t (number_of_rows)
              WHERE pc.is_anti_matter = 1'

  /*SELECT @SQL*/
  INSERT INTO #tmp_AntiMatterColumns
  EXEC (@SQL)
  
  FETCH NEXT FROM c_databases
  into @database_name
END
CLOSE c_databases
DEALLOCATE c_databases

SELECT 'Check 39 - Check if there are "anti-matter" columns, as this may cause issues with update stats.' AS [Info],
       *
FROM #tmp_AntiMatterColumns
ORDER BY number_of_rows DESC


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

SELECT * FROM #tmp_auto_stats
ORDER BY duration_ms desc

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
