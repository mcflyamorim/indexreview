SET NOCOUNT ON; SET ARITHABORT OFF; SET ARITHIGNORE ON;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
SET LOCK_TIMEOUT 1000; /*1 seconds*/
SET DATEFORMAT MDY

------------------------
-- Uptime information --
------------------------
DECLARE @sqlcmd NVARCHAR(MAX),
        @params NVARCHAR(600),
        @sqlmajorver INT;
DECLARE @UpTime VARCHAR(12),@StartDate DATETIME

SELECT @sqlmajorver = CONVERT(INT, (@@microsoftversion / 0x1000000) & 0xff);

IF @sqlmajorver < 10
BEGIN
    SET @sqlcmd
        = N'SELECT @UpTimeOUT = DATEDIFF(mi, login_time, GETDATE()), @StartDateOUT = login_time FROM master..sysprocesses (NOLOCK) WHERE spid = 1';
END;
ELSE
BEGIN
    SET @sqlcmd
        = N'SELECT @UpTimeOUT = DATEDIFF(mi,sqlserver_start_time,GETDATE()), @StartDateOUT = sqlserver_start_time FROM sys.dm_os_sys_info (NOLOCK)';
END;

SET @params = N'@UpTimeOUT VARCHAR(12) OUTPUT, @StartDateOUT DATETIME OUTPUT';

EXECUTE sp_executesql @sqlcmd,
                      @params,
                      @UpTimeOUT = @UpTime OUTPUT,
                      @StartDateOUT = @StartDate OUTPUT;

IF OBJECT_ID('tempdb.dbo.tmpIndexCheckSummary') IS NOT NULL
    DROP TABLE tempdb.dbo.tmpIndexCheckSummary;
WITH CTE_1
AS (
   SELECT CONVERT(VARCHAR(8000), 'SQL Server instance startup time: ' + CONVERT(VARCHAR(30), @StartDate, 20)) AS [info],
          CONVERT(VARCHAR(200), CONVERT(VARCHAR(4), @UpTime / 60 / 24) + 'd ' + CONVERT(VARCHAR(4), @UpTime / 60 % 24) + 'hr ' + CONVERT(VARCHAR(4), @UpTime % 60) + 'min') AS [result],
          'NA' AS prioritycol,
          'NA' AS more_info,
          '' AS quick_fix
   UNION ALL

   SELECT CONVERT(VARCHAR(8000), 'Total number of databases: ') AS [info],
          '- ' + CONVERT(VARCHAR(200), COUNT(DISTINCT Database_ID)) + ' -' AS [result],
          'NA' AS prioritycol,
          'NA' AS more_info,
          'NA' AS quick_fix
   FROM tempdb.dbo.Tab_GetIndexInfo
   UNION ALL

   SELECT CONVERT(VARCHAR(8000), 'Total number of tables: ') AS [info],
          '- ' + CONVERT(VARCHAR(200),COUNT(DISTINCT Database_Name + Schema_Name + Table_Name)) + ' -' AS [result],
          'NA' AS prioritycol,
          'NA' AS more_info,
          'NA' AS quick_fix
   FROM tempdb.dbo.Tab_GetIndexInfo
   UNION ALL

   SELECT CONVERT(VARCHAR(8000), 'Total number of indexes: ') AS [info],
          '- ' + CONVERT(VARCHAR(200), COUNT(*)) + ' -' AS [result],
          'NA' AS prioritycol,
          'NA' AS more_info,
          'NA' AS quick_fix
   FROM tempdb.dbo.Tab_GetIndexInfo
   UNION ALL

   SELECT TOP 1 
          CONVERT(VARCHAR(8000), 'Table with biggest number of indexes: ' + QUOTENAME(Database_Name) + '.' + QUOTENAME(Schema_Name) + '.' + QUOTENAME(Table_Name) + ' with ' + CONVERT(VARCHAR, Number_Of_Indexes_On_Table) + ' indexes.' ) AS [info],
          '- ' + CONVERT(VARCHAR(200), Number_Of_Indexes_On_Table) + ' -' AS [result],
          'NA' AS prioritycol,
          'Check29' AS more_info,
          'NA' AS quick_fix
   FROM tempdb.dbo.tmpIndexCheck29
   ORDER BY Number_Of_Indexes_On_Table DESC
   UNION ALL

   SELECT TOP 1 
          CONVERT(VARCHAR(8000), 'Number of duplicated indexes on table with biggest number of indexes: ' + QUOTENAME(tmpIndexCheck3.Database_Name) + '.' + QUOTENAME(tmpIndexCheck3.Schema_Name) + '.' + QUOTENAME(tmpIndexCheck3.Table_Name) + ' with ' + CONVERT(VARCHAR(200), COUNT(DISTINCT CONVERT(VARCHAR(MAX), tmpIndexCheck3.Overlapped_Index))) + ' duplicated indexes.' ) AS [info],
          CONVERT(VARCHAR(200), COUNT(DISTINCT CONVERT(VARCHAR(MAX), tmpIndexCheck3.Overlapped_Index))) AS [result],
          'NA' AS prioritycol,
          'Check29' AS more_info,
          'NA' AS quick_fix
   FROM tempdb.dbo.tmpIndexCheck3
   INNER JOIN (SELECT TOP 1 
                       t1.*
                FROM tempdb.dbo.tmpIndexCheck29 AS t1
                ORDER BY t1.Number_Of_Indexes_On_Table DESC) AS t1
   ON  t1.Database_Name = tmpIndexCheck3.Database_Name
   AND t1.Schema_Name = tmpIndexCheck3.Schema_Name
   AND t1.Table_Name = tmpIndexCheck3.Table_Name
   GROUP BY tmpIndexCheck3.Database_Name, tmpIndexCheck3.Schema_Name, tmpIndexCheck3.Table_Name
   UNION ALL

   SELECT TOP 1 
          CONVERT(VARCHAR(8000), 'Biggest key: ' + LOWER(Index_Type) + ' index ' + QUOTENAME(Index_Name) + ' on table ' + QUOTENAME(Database_Name) + '.' + QUOTENAME(Schema_Name) + '.' + QUOTENAME(Table_Name) + ' has ' + CONVERT(VARCHAR, KeyColumnsCount) + ' columns.' ) AS [info],
          '- ' + CONVERT(VARCHAR(200), KeyColumnsCount) + ' -' AS [result],
          'NA' AS prioritycol,
          'Check29' AS more_info,
          'NA' AS quick_fix
   FROM tempdb.dbo.tmpIndexCheck28
   ORDER BY KeyColumnsCount DESC
   UNION ALL

   SELECT TOP 1 
          CONVERT(VARCHAR(8000), 'Most accessed table: '+QUOTENAME(Database_Name) + '.' + QUOTENAME(Schema_Name) + '.' + QUOTENAME(Table_Name) + ' (' + REPLACE(CONVERT(VARCHAR(30), CONVERT(MONEY, Number_Rows), 1), '.00', '')  + ' rows) with '+ CONVERT(VARCHAR, user_seeks + user_scans + user_lookups + user_updates) + ' accesses ('+ CONVERT(VARCHAR, CONVERT(NUMERIC(25,2), tab1.avg_of_access_per_minute_based_on_index_usage_dmv)) +' per min): ' ) AS [info],
          '- ' + CONVERT(VARCHAR(200), CONVERT(NUMERIC(25,2), tab1.avg_of_access_per_minute_based_on_index_usage_dmv)) + ' -' AS [result],
          'NA' AS prioritycol,
          'Check10' AS more_info,
          'NA' AS quick_fix
   FROM tempdb.dbo.Tab_GetIndexInfo AS a
   CROSS APPLY (SELECT CONVERT(VARCHAR(200), CONVERT(NUMERIC(25,2), user_seeks + user_scans + user_lookups + user_updates) / 
                                CASE DATEDIFF(mi, (SELECT create_date FROM sys.databases WHERE name = 'tempdb'), GETDATE())
                                  WHEN 0 THEN 1
                                  ELSE DATEDIFF(mi, (SELECT create_date FROM sys.databases WHERE name = 'tempdb'), GETDATE())
                                END)) AS tab1(avg_of_access_per_minute_based_on_index_usage_dmv)
   WHERE user_seeks + user_scans + user_lookups + user_updates > 0
   ORDER BY tab1.avg_of_access_per_minute_based_on_index_usage_dmv DESC
   UNION ALL

   SELECT CONVERT(VARCHAR(8000), 'Found a index maintenance plan: ') AS [info],
          CONVERT(VARCHAR(200), CASE WHEN EXISTS(SELECT 1 FROM tempdb.dbo.tmpIndexCheck17 WHERE comment = 'OK') THEN 1 ELSE 0 END) AS [result],
          'High' AS prioritycol,
          'Check40' AS more_info,
          'NA' AS quick_fix
   UNION ALL

   SELECT TOP 1 
          CONVERT(VARCHAR(8000), 'Percent of indexes with frag >= 30% (only considering tables >= 10mb): ' ) AS [info],
          CONVERT(VARCHAR(200), CONVERT(NUMERIC(25, 2), COUNT(CASE WHEN avg_fragmentation_in_percent >= 30 THEN 1 ELSE NULL END) / CONVERT(NUMERIC(25, 2), COUNT(*)) * 100)) AS [result],
          'High' AS prioritycol,
          'Check2' AS more_info,
          'NA' AS quick_fix
   FROM tempdb.dbo.tmpIndexCheck2
   WHERE ReservedSizeInMB >= 10
   UNION ALL

   SELECT TOP 1 
          CONVERT(VARCHAR(8000), 'Frag of most scanned index ('+QUOTENAME(Database_Name) + '.' + QUOTENAME(Schema_Name) + '.' + QUOTENAME(Table_Name)+' with '+CONVERT(VARCHAR, user_scans)+' scans) (only considering tables >= 10mb): ' ) AS [info],
          CONVERT(VARCHAR(200), avg_fragmentation_in_percent) AS [result],
          'High' AS prioritycol,
          'Check27' AS more_info,
          'NA' AS quick_fix
   FROM tempdb.dbo.tmpIndexCheck27
   WHERE ReservedSizeInMB >= 10
   ORDER BY user_scans DESC
   UNION ALL

   SELECT TOP 1 
          CONVERT(VARCHAR(8000), 'Expensive scans on table with more than 10mi rows ('+QUOTENAME(Database_Name) + '.' + QUOTENAME(Schema_Name) + '.' + QUOTENAME(Table_Name)+' with '+CONVERT(VARCHAR, user_scans)+' scans): ' ) AS [info],
          CONVERT(VARCHAR(200), user_scans) AS [result],
          'High' AS prioritycol,
          'Check27' AS more_info,
          'NA' AS quick_fix
   FROM tempdb.dbo.tmpIndexCheck27
   WHERE current_number_of_rows_table >= 10000000 /*10mi*/
   ORDER BY user_scans DESC
   UNION ALL

   SELECT CONVERT(VARCHAR(8000), 'Number of duplicated indexes: ' ) AS [info],
          CONVERT(VARCHAR(200), COUNT(DISTINCT CONVERT(VARCHAR(MAX), Exact_Duplicated))) AS [result],
          'High' AS prioritycol,
          'Check3' AS more_info,
          'NA' AS quick_fix
   FROM tempdb.dbo.tmpIndexCheck3
   UNION ALL

   SELECT CONVERT(VARCHAR(8000), 'Number of overlapped indexes: ' ) AS [info],
          CONVERT(VARCHAR(200), COUNT(DISTINCT CONVERT(VARCHAR(MAX), Overlapped_Index))) AS [result],
          'High' AS prioritycol,
          'Check3' AS more_info,
          'NA' AS quick_fix
   FROM tempdb.dbo.tmpIndexCheck3
   UNION ALL

   SELECT CONVERT(VARCHAR(8000), 'Number of indexes with very low page density (avg rows per page <= 20) (only considering tables >= 1000 rows): ' ) AS [info],
          CONVERT(VARCHAR(200), COUNT(*)) AS [result],
          'High' AS prioritycol,
          'Check1' AS more_info,
          'NA' AS quick_fix
   FROM tempdb.dbo.tmpIndexCheck1
   WHERE [Avg rows per page] <= 20
   AND current_number_of_rows_table >= 1000
   UNION ALL

   SELECT CONVERT(VARCHAR(8000), 'Number of non-used indexes: ' ) AS [info],
          CONVERT(VARCHAR(200), COUNT(*)) AS [result],
          'High' AS prioritycol,
          'Check10' AS more_info,
          'NA' AS quick_fix
   FROM tempdb.dbo.tmpIndexCheck10
   WHERE [Comment 2] <> 'OK'
   UNION ALL

   SELECT CONVERT(VARCHAR(8000), 'Number of non-used indexes with update: ' ) AS [info],
          CONVERT(VARCHAR(200), COUNT(*)) AS [result],
          'High' AS prioritycol,
          'Check10' AS more_info,
          'NA' AS quick_fix
   FROM tempdb.dbo.tmpIndexCheck10
   WHERE [Comment 1] <> 'OK'
   UNION ALL

   SELECT CONVERT(VARCHAR(8000), 'Number of rarely used indexes: ' ) AS [info],
          CONVERT(VARCHAR(200), COUNT(*)) AS [result],
          'High' AS prioritycol,
          'Check10' AS more_info,
          'NA' AS quick_fix
   FROM tempdb.dbo.tmpIndexCheck10
   WHERE [Comment 3] <> 'OK'
   UNION ALL

   SELECT CONVERT(VARCHAR(8000), 'Number of non-indexed FKs: ' ) AS [info],
          CONVERT(VARCHAR(200), COUNT(*)) AS [result],
          'Medium' AS prioritycol,
          'Check16' AS more_info,
          'NA' AS quick_fix
   FROM tempdb.dbo.tmpIndexCheck16
   UNION ALL

   SELECT CONVERT(VARCHAR(8000), 'Number of indexes with fillfactor <= 80: ' ) AS [info],
          CONVERT(VARCHAR(200), COUNT(*)) AS [result],
          'High' AS prioritycol,
          'Check4' AS more_info,
          'NA' AS quick_fix
   FROM tempdb.dbo.tmpIndexCheck4
  WHERE fill_factor <= 80
   UNION ALL

   SELECT CONVERT(VARCHAR(8000), 'Number of modules with hard-coded indexes: ' ) AS [info],
          CONVERT(VARCHAR(200), COUNT(*)) AS [result],
          'Low' AS prioritycol,
          'Check9' AS more_info,
          'NA' AS quick_fix
   FROM tempdb.dbo.tmpIndexCheck9
   UNION ALL

   SELECT CONVERT(VARCHAR(8000), 'Number of missing index DMV: ' ) AS [info],
          CONVERT(VARCHAR(200), COUNT(*)) AS [result],
          'High' AS prioritycol,
          'Check21' AS more_info,
          'NA' AS quick_fix
   FROM tempdb.dbo.tmpIndexCheck21
   UNION ALL

   SELECT CONVERT(VARCHAR(8000), 'Number of missing index plan cache: ' ) AS [info],
          CONVERT(VARCHAR(200), COUNT(*)) AS [result],
          'High' AS prioritycol,
          'Check22' AS more_info,
          'NA' AS quick_fix
   FROM tempdb.dbo.tmpIndexCheck22
   UNION ALL

   SELECT CONVERT(VARCHAR(8000), 'Number of indexes with key size bigger then the limit: ' ) AS [info],
          CONVERT(VARCHAR(200), COUNT(*)) AS [result],
          'Low' AS prioritycol,
          'Check11' AS more_info,
          'NA' AS quick_fix
   FROM tempdb.dbo.tmpIndexCheck11
   UNION ALL

   SELECT CONVERT(VARCHAR(8000), 'Number of hypothetical indexes: ' ) AS [info],
          CONVERT(VARCHAR(200), COUNT(*)) AS [result],
          'Low' AS prioritycol,
          'Check12' AS more_info,
          'NA' AS quick_fix
   FROM tempdb.dbo.tmpIndexCheck12
   WHERE [Database_Name] IS NOT NULL
   UNION ALL

   SELECT CONVERT(VARCHAR(8000), 'Number of disabled indexes: ' ) AS [info],
          CONVERT(VARCHAR(200), COUNT(*)) AS [result],
          'Medium' AS prioritycol,
          'Check13' AS more_info,
          'NA' AS quick_fix
   FROM tempdb.dbo.tmpIndexCheck13
   UNION ALL

   SELECT CONVERT(VARCHAR(8000), 'Number of non-unique clustered indexes: ' ) AS [info],
          CONVERT(VARCHAR(200), COUNT(*)) AS [result],
          'Low' AS prioritycol,
          'Check14' AS more_info,
          'NA' AS quick_fix
   FROM tempdb.dbo.tmpIndexCheck14
   UNION ALL

   SELECT CONVERT(VARCHAR(8000), 'Number of indexes using a GUID on key: ' ) AS [info],
          CONVERT(VARCHAR(200), COUNT(*)) AS [result],
          'Medium' AS prioritycol,
          'Check15' AS more_info,
          'NA' AS quick_fix
   FROM tempdb.dbo.tmpIndexCheck15
   UNION ALL

   SELECT CONVERT(VARCHAR(8000), 'Number of tables that do not have a clustered index, but have non-clustered index: ' ) AS [info],
          CONVERT(VARCHAR(200), COUNT(*)) AS [result],
          'Medium' AS prioritycol,
          'Check18' AS more_info,
          'NA' AS quick_fix
   FROM tempdb.dbo.tmpIndexCheck18
   WHERE [DatabaseName] IS NOT NULL
   UNION ALL

   SELECT CONVERT(VARCHAR(8000), 'Number of heaps (only considering non-empty tables): ' ) AS [info],
          CONVERT(VARCHAR(200), COUNT(*)) AS [result],
          'Medium' AS prioritycol,
          'Check18' AS more_info,
          'NA' AS quick_fix
   FROM tempdb.dbo.tmpIndexCheck19
   WHERE current_number_of_rows_table > 0
   UNION ALL

   SELECT CONVERT(VARCHAR(8000), 'Number of partitioned tables with non-aligned index: ' ) AS [info],
          CONVERT(VARCHAR(200), COUNT(*)) AS [result],
          'High' AS prioritycol,
          'Check20' AS more_info,
          'NA' AS quick_fix
   FROM tempdb.dbo.tmpIndexCheck20
   WHERE [DatabaseName] IS NOT NULL
   UNION ALL

   SELECT CONVERT(VARCHAR(8000), 'Number of indexes left over indexes or with bad naming convention: ' ) AS [info],
          CONVERT(VARCHAR(200), COUNT(*)) AS [result],
          'High' AS prioritycol,
          'Check23' AS more_info,
          'NA' AS quick_fix
   FROM tempdb.dbo.tmpIndexCheck23
   UNION ALL

   SELECT CONVERT(VARCHAR(8000), 'Number of tables with nonclust indexes good candidates to be recreated with clust: ') AS [info],
          CONVERT(VARCHAR(200), COUNT(*)) AS [result],
          'Medium' AS prioritycol,
          'Check46' AS more_info,
          'NA' AS quick_fix
   FROM (SELECT DISTINCT table_name FROM tempdb.dbo.tmpIndexCheck46 WHERE comment <> 'OK') AS t
   UNION ALL

   SELECT CONVERT(VARCHAR(8000), 'Number of clustered indexes with singleton lookup ratio >= 90%: ') AS [info],
          CONVERT(VARCHAR(200), COUNT(*)) AS [result],
          'Medium' AS prioritycol,
          'Check31' AS more_info,
          'NA' AS quick_fix
   FROM tempdb.dbo.tmpIndexCheck31
   WHERE singleton_lookup_ratio >= 90
   UNION ALL

   SELECT CONVERT(VARCHAR(8000), 'Number of tables with index size is greater than the table size: ') AS [info],
          CONVERT(VARCHAR(200), COUNT(*)) AS [result],
          'Medium' AS prioritycol,
          'Check32' AS more_info,
          'NA' AS quick_fix
   FROM tempdb.dbo.tmpIndexCheck32
   WHERE [Comment] <> 'OK'
   UNION ALL

   SELECT CONVERT(VARCHAR(8000), 'Number of tables row/page lock disabled: ') AS [info],
          CONVERT(VARCHAR(200), COUNT(*)) AS [result],
          'High' AS prioritycol,
          'Check33' AS more_info,
          'NA' AS quick_fix
   FROM tempdb.dbo.tmpIndexCheck33
   UNION ALL

   SELECT CONVERT(VARCHAR(8000), 'Number of non-partitioned tables with more than 10mi rows: ') AS [info],
          CONVERT(VARCHAR(200), COUNT(*)) AS [result],
          'Medium' AS prioritycol,
          'Check34' AS more_info,
          'NA' AS quick_fix
   FROM tempdb.dbo.tmpIndexCheck34
   UNION ALL

   SELECT CONVERT(VARCHAR(8000), 'Number of non-compressed indexes: ') AS [info],
          CONVERT(VARCHAR(200), COUNT(*)) AS [result],
          'High' AS prioritycol,
          'Check35' AS more_info,
          'NA' AS quick_fix
   FROM tempdb.dbo.tmpIndexCheck35
   UNION ALL

   SELECT CONVERT(VARCHAR(8000), 'Number of indexes on ascending columns: ') AS [info],
          CONVERT(VARCHAR(200), COUNT(*)) AS [result],
          'Medium' AS prioritycol,
          'Check36' AS more_info,
          'NA' AS quick_fix
   FROM tempdb.dbo.tmpIndexCheck36
   UNION ALL

   SELECT CONVERT(VARCHAR(8000), 'Number of indexes on ascending columns with fillfactor <> 100: ') AS [info],
          CONVERT(VARCHAR(200), COUNT(*)) AS [result],
          'Medium' AS prioritycol,
          'Check36' AS more_info,
          'NA' AS quick_fix
   FROM tempdb.dbo.tmpIndexCheck36
   WHERE Fill_factor NOT IN (0, 100)
   UNION ALL

   SELECT CONVERT(VARCHAR(8000), 'Total of free memory used by indexes on ascending columns with fillfactor <> 100: ' + CONVERT(VARCHAR(200), ISNULL(CONVERT(NUMERIC(25, 2), SUM(Buffer_Pool_FreeSpace_MB) / 1024.),0)) + 'gb') AS [info],
          CONVERT(VARCHAR(200), ISNULL(CONVERT(NUMERIC(25, 2), SUM(Buffer_Pool_FreeSpace_MB) / 1024.),0)) AS [result],
          'Medium' AS prioritycol,
          'Check36' AS more_info,
          'NA' AS quick_fix
   FROM tempdb.dbo.tmpIndexCheck36
   WHERE Fill_factor NOT IN (0, 100)
   UNION ALL

   SELECT CONVERT(VARCHAR(8000), 'Number of tables with more indexes than columns: ' ) AS [info],
          CONVERT(VARCHAR(200), COUNT(*)) AS [result],
          'High' AS prioritycol,
          'Check38' AS more_info,
          'NA' AS quick_fix
   FROM tempdb.dbo.tmpIndexCheck38
   WHERE [DatabaseName] IS NOT NULL
   UNION ALL

   SELECT CONVERT(VARCHAR(8000), 'Number of tables with identity cols approaching max limit: ' ) AS [info],
          CONVERT(VARCHAR(200), COUNT(*)) AS [result],
          'High' AS prioritycol,
          'Check39' AS more_info,
          'NA' AS quick_fix
   FROM tempdb.dbo.tmpIndexCheck39
   WHERE [Comment] <> 'OK'
   UNION ALL

   SELECT CONVERT(VARCHAR(8000), 'Number of nonclustered indexes that are good candidates to be recreated as a clustered: ' ) AS [info],
          CONVERT(VARCHAR(200), COUNT(*)) AS [result],
          'Medium' AS prioritycol,
          'Check41' AS more_info,
          'NA' AS quick_fix
   FROM tempdb.dbo.tmpIndexCheck41
   WHERE [Comment] <> 'OK'
   UNION ALL

   SELECT CONVERT(VARCHAR(8000), 'Number of tables storing >= 10 years of data: ' ) AS [info],
          CONVERT(VARCHAR(200), COUNT(DISTINCT Database_Name + Schema_Name + Table_Name)) AS [result],
          'High' AS prioritycol,
          'Check42' AS more_info,
          'NA' AS quick_fix
   FROM tempdb.dbo.tmpIndexCheck42
   WHERE YearsCnt >= 10
   UNION ALL

   SELECT TOP 1 
          CONVERT(VARCHAR(8000), 'Table with biggest row lock wait: ' + QUOTENAME(Database_Name) + '.' + QUOTENAME(Schema_Name) + '.' + QUOTENAME(Table_Name)+' with '+ CONVERT(VARCHAR, row_lock_wait_count) + ' events, total time of ' + CONVERT(VARCHAR(10), ((total_row_lock_wait_in_ms) / 1000) / 86400) + 'd:' + CONVERT(VARCHAR(20), DATEADD(s, ((total_row_lock_wait_in_ms) / 1000), 0), 108)  + ' and avg of ' + CONVERT(VARCHAR, avg_row_lock_wait_in_ms)  + 'ms per wait: ' ) AS [info],
          CONVERT(VARCHAR(200), total_row_lock_wait_in_ms) AS [result],
          'High' AS prioritycol,
          'Check37' AS more_info,
          'NA' AS quick_fix
   FROM tempdb.dbo.tmpIndexCheck37
   WHERE total_row_lock_wait_in_ms > 0
   ORDER BY total_row_lock_wait_in_ms DESC
   UNION ALL

   SELECT TOP 1 
          CONVERT(VARCHAR(8000), 'Table with biggest page lock wait: ' + QUOTENAME(Database_Name) + '.' + QUOTENAME(Schema_Name) + '.' + QUOTENAME(Table_Name)+' with '+ CONVERT(VARCHAR, page_lock_count) + ' events, total time of ' + CONVERT(VARCHAR(10), ((total_page_lock_wait_in_ms) / 1000) / 86400) + 'd:' + CONVERT(VARCHAR(20), DATEADD(s, ((total_page_lock_wait_in_ms) / 1000), 0), 108)  + ' and avg of ' + CONVERT(VARCHAR, avg_page_lock_wait_in_ms)  + 'ms per wait: ' ) AS [info],
          CONVERT(VARCHAR(200), total_page_lock_wait_in_ms) AS [result],
          'High' AS prioritycol,
          'Check37' AS more_info,
          'NA' AS quick_fix
   FROM tempdb.dbo.tmpIndexCheck37
   WHERE total_page_lock_wait_in_ms > 0
   ORDER BY total_page_lock_wait_in_ms DESC
   UNION ALL

   SELECT TOP 1 
          CONVERT(VARCHAR(8000), 'Table with biggest page I/O latch: ' + QUOTENAME(Database_Name) + '.' + QUOTENAME(Schema_Name) + '.' + QUOTENAME(Table_Name)+' with '+ CONVERT(VARCHAR, total_page_io_latch_wait_in_ms) + ' events, total time of ' + CONVERT(VARCHAR(10), ((total_page_io_latch_wait_in_ms) / 1000) / 86400) + 'd:' + CONVERT(VARCHAR(20), DATEADD(s, ((total_page_io_latch_wait_in_ms) / 1000), 0), 108)  + ' and avg of ' + CONVERT(VARCHAR, page_io_latch_avg_wait_ms)  + 'ms per wait: ' ) AS [info],
          CONVERT(VARCHAR(200), total_page_io_latch_wait_in_ms) AS [result],
          'High' AS prioritycol,
          'Check24' AS more_info,
          'NA' AS quick_fix
   FROM tempdb.dbo.tmpIndexCheck24
   WHERE total_page_io_latch_wait_in_ms > 0
   ORDER BY total_page_io_latch_wait_in_ms DESC
   UNION ALL

   SELECT TOP 1 
          CONVERT(VARCHAR(8000), 'Table with biggest memory page latch: ' + QUOTENAME(Database_Name) + '.' + QUOTENAME(Schema_Name) + '.' + QUOTENAME(Table_Name)+' with '+ CONVERT(VARCHAR, total_page_latch_wait_in_ms) + ' events, total time of ' + CONVERT(VARCHAR(10), ((total_page_latch_wait_in_ms) / 1000) / 86400) + 'd:' + CONVERT(VARCHAR(20), DATEADD(s, ((total_page_latch_wait_in_ms) / 1000), 0), 108)  + ' and avg of ' + CONVERT(VARCHAR, page_latch_avg_wait_ms)  + 'ms per wait: ' ) AS [info],
          CONVERT(VARCHAR(200), total_page_latch_wait_in_ms) AS [result],
          'High' AS prioritycol,
          'Check30' AS more_info,
          'NA' AS quick_fix
   FROM tempdb.dbo.tmpIndexCheck30
   WHERE total_page_latch_wait_in_ms > 0
   ORDER BY total_page_latch_wait_in_ms DESC
   UNION ALL

   SELECT TOP 1 
          CONVERT(VARCHAR(8000), 'Table with biggest memory used space: ' + QUOTENAME(Database_Name) + '.' + QUOTENAME(Schema_Name) + '.' + QUOTENAME(Table_Name) + ' (Current compression = ' + data_compression_desc + ') with '+ CONVERT(VARCHAR, CONVERT(NUMERIC(25, 2), Buffer_Pool_SpaceUsed_MB / 1024.)) + 'gb' ) AS [info],
          CONVERT(VARCHAR(200), CONVERT(NUMERIC(25, 2), Buffer_Pool_SpaceUsed_MB / 1024.)) AS [result],
          'High' AS prioritycol,
          'Check25' AS more_info,
          'NA' AS quick_fix
   FROM tempdb.dbo.tmpIndexCheck25
   ORDER BY Buffer_Pool_SpaceUsed_MB DESC
   UNION ALL

   SELECT TOP 1 
          CONVERT(VARCHAR(8000), 'Table with biggest memory free space: ' + QUOTENAME(Database_Name) + '.' + QUOTENAME(Schema_Name) + '.' + QUOTENAME(Table_Name) + ' (Current compression = ' + data_compression_desc + ') with Used='+ CONVERT(VARCHAR, CONVERT(NUMERIC(25, 2), Buffer_Pool_SpaceUsed_MB / 1024.)) + 'gb and Free=' + CONVERT(VARCHAR, CONVERT(NUMERIC(25, 2), Buffer_Pool_FreeSpace_MB / 1024.)) + 'gb' ) AS [info],
          CONVERT(VARCHAR(200), CONVERT(NUMERIC(25, 2), Buffer_Pool_FreeSpace_MB / 1024.)) AS [result],
          'High' AS prioritycol,
          'Check25' AS more_info,
          'NA' AS quick_fix
   FROM tempdb.dbo.tmpIndexCheck25
   ORDER BY Buffer_Pool_FreeSpace_MB DESC
   UNION ALL

   SELECT CONVERT(VARCHAR(8000), 'BP space used for TOP 10 indexes: ' + CONVERT(VARCHAR, CONVERT(NUMERIC(25, 2), SUM(Buffer_Pool_SpaceUsed_MB) / 1024.)) + 'gb') AS [info],
          CONVERT(VARCHAR(200), CONVERT(NUMERIC(25, 2), SUM(Buffer_Pool_SpaceUsed_MB) / 1024.)) AS [result],
          'High' AS prioritycol,
          'Check25' AS more_info,
          'NA' AS quick_fix
   FROM (SELECT TOP 10 * FROM tempdb.dbo.tmpIndexCheck25 ORDER BY Buffer_Pool_SpaceUsed_MB DESC) AS t
   UNION ALL

   SELECT CONVERT(VARCHAR(8000), 'BP total of free space used: ' + CONVERT(VARCHAR, CONVERT(NUMERIC(25, 2), SUM(Buffer_Pool_FreeSpace_MB) / 1024.)) + 'gb') AS [info],
          CONVERT(VARCHAR(200), CONVERT(NUMERIC(25, 2), SUM(Buffer_Pool_FreeSpace_MB) / 1024.)) AS [result],
          'High' AS prioritycol,
          'Check25' AS more_info,
          'NA' AS quick_fix
   FROM tempdb.dbo.tmpIndexCheck25
   UNION ALL

   SELECT TOP 1 
          CONVERT(VARCHAR(8000), 'High number of lock escalations: '+QUOTENAME(Database_Name) + '.' + QUOTENAME(Schema_Name) + '.' + QUOTENAME(Table_Name)+' with '+CONVERT(VARCHAR, index_lock_escaltion_count)+' escalations: ' ) AS [info],
          CONVERT(VARCHAR(200), index_lock_escaltion_count) AS [result],
          'High' AS prioritycol,
          'Check26' AS more_info,
          'NA' AS quick_fix
   FROM tempdb.dbo.tmpIndexCheck26
   ORDER BY index_lock_escaltion_count DESC
   UNION ALL

   SELECT CONVERT(VARCHAR(8000), 'Non default instance config is used: ') AS [info],
          CONVERT(VARCHAR(200), CASE WHEN EXISTS(SELECT 1 FROM tempdb.dbo.tmpIndexCheck17 WHERE comment <> 'OK') THEN 1 ELSE 0 END) AS [result],
          'Medium' AS prioritycol,
          'Check26' AS more_info,
          'NA' AS quick_fix
   )
SELECT *
INTO tempdb.dbo.tmpIndexCheckSummary
FROM CTE_1;

SELECT *
FROM tempdb.dbo.tmpIndexCheckSummary;
