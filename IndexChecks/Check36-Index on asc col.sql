/* 
Check36 - Indexes in a key column set to ascending

Description:
Review reported indexes and make sure the frequency you update the index statistics is enough to provide accurate information. Statistics on ascending or descending key columns, such as IDENTITY or real-time timestamp columns, might require more frequent statistics updates than the Query Optimizer performs. 
Insert operations append new values to ascending or descending columns. The number of rows added might be too small to trigger a statistics update. If statistics are not up-to-date and queries select from the most recently added rows, the current statistics will not have cardinality estimates for these new values. This can result in inaccurate cardinality estimates and slow query performance.
For example, a query that selects from the most recent sales order dates will have inaccurate cardinality estimates if the statistics are not updated to include cardinality estimates for the most recent sales order dates.

Estimated Benefit:
Medium

Estimated Effort:
High

Recommendation:
Quick recommendation:
Review queries using reported indexes and make sure you’re update statistics on those indexes.

Detailed recommendation:
Review queries using reported indexes and check if queries using those indexes are trying to read latest inserted rows. Check if queries are using predicates beyond the RANGE_HI_KEY value of the existing statistics, if so, make sure you've a script to update the statistic more often to guarantee those queries will have Information about newest records.
If you can't spend time looking at all queries using those tables, you can create a job to update those statistics more often. Make sure your script is smart enough to only run update if number of modified rows changed. Probably an update with sample is enough, as long as you do a bigger sample in the regular maintenance window update.

Note: On KB3189645 (SQL2014 SP1 CU9(12.00.4474) and SP2 CU2(12.00.5532)) filtered indexes are exempted from quickstats queries because it had a bug with filtered indexes and columnstore, but, that ended up fixing another problem that when the quickstats query was issued for filtered index stats it has no filter, which was making a full scan (unless a nonfiltered index with the same first column happens to be around to help).

Note: Those are good candidates for partitioning and purge data by the ascending col.
*/



SET NOCOUNT ON; SET ARITHABORT OFF; SET ARITHIGNORE ON; 
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
SET LOCK_TIMEOUT 60000; /*60 seconds*/
SET DATEFORMAT MDY

IF OBJECT_ID('dbo.tmpIndexCheck36') IS NOT NULL
  DROP TABLE dbo.tmpIndexCheck36


IF OBJECT_ID('tempdb.dbo.#TMP1') IS NOT NULL
  DROP TABLE #TMP1

SELECT TOP 1000
       'Check36 - Indexes in a key column set to ascending' AS [Info],
       a.Database_Name,
       a.Schema_Name,
       a.Table_Name,
       a.Index_Name,
       a.Index_Type,
       a.indexed_columns,
       a.Number_Rows AS current_number_of_rows_table,
       a.ReservedSizeInMB AS [Table Size],
       a.fill_factor,
       a.Buffer_Pool_SpaceUsed_MB,
       a.Buffer_Pool_FreeSpace_MB,
       CONVERT(NUMERIC(18, 2), (a.Buffer_Pool_FreeSpace_MB / CASE WHEN a.Buffer_Pool_SpaceUsed_MB = 0 THEN 1 ELSE a.Buffer_Pool_SpaceUsed_MB END) * 100) AS Buffer_Pool_FreeSpace_Percent,
       a.TableHasLOB,
       UpdateStatsCmd = N'DBCC SHOW_STATISTICS (''' + QUOTENAME(a.Database_Name) + '.' + QUOTENAME(a.Schema_Name) + N'.' + QUOTENAME(a.Table_Name) + N''', '+ QUOTENAME(a.Index_Name) +') WITH NO_INFOMSGS;',
       CONVERT(VARCHAR(200),NULL) AS LeadingType
  INTO #TMP1
  FROM dbo.Tab_GetIndexInfo a
 --WHERE a.Number_Rows >= 10000000
ORDER BY a.Number_Rows DESC, 
         a.Database_Name,
         a.Schema_Name,
         a.Table_Name,
         a.Index_Name


IF OBJECT_ID('tempdb.dbo.#TMPShowStatistics') IS NOT NULL
    DROP TABLE #TMPShowStatistics;

CREATE TABLE #TMPShowStatistics(
  [TF2388_Updated] datetime,
  [TF2388_Table Cardinality] BigInt,
  [TF2388_Snapshot Ctr] BigInt,
  [TF2388_Steps] BigInt,
  [TF2388_Density] Float,
  [TF2388_Rows Above] Float,
  [TF2388_Rows Below] Float,
  [TF2388_Squared Variance Error] Float,
  [TF2388_Inserts Since Last Update] Float,
  [TF2388_Deletes Since Last Update] Float,
  [TF2388_Leading column Type] NVarChar(200)
)

declare @UpdateStatsCmd VARCHAR(8000), @Msg VARCHAR(8000)

BEGIN TRY
  DBCC TRACEON(2388) WITH NO_INFOMSGS;

  DECLARE c_StatsCmd CURSOR read_only FOR
      SELECT UpdateStatsCmd FROM #TMP1
  OPEN c_StatsCmd

  FETCH NEXT FROM c_StatsCmd
  into @UpdateStatsCmd
  WHILE @@FETCH_STATUS = 0
  BEGIN
    /*SELECT @SQL*/
    BEGIN TRY
      INSERT INTO #TMPShowStatistics
      EXEC (@UpdateStatsCmd)

      UPDATE #TMP1 SET LeadingType = #TMPShowStatistics.[TF2388_Leading column Type]
      FROM #TMP1 t
      INNER JOIN #TMPShowStatistics
      ON t.UpdateStatsCmd = @UpdateStatsCmd
      WHERE #TMPShowStatistics.[TF2388_Leading column Type] IS NOT NULL  
    END TRY
		  BEGIN CATCH
			   SELECT @Msg = 'Error trying to run ' + @UpdateStatsCmd
      RAISERROR (@Msg, 0,0) WITH NOWAIT
		  END CATCH

    TRUNCATE TABLE #TMPShowStatistics
    FETCH NEXT FROM c_StatsCmd
    into @UpdateStatsCmd
  END
  CLOSE c_StatsCmd
  DEALLOCATE c_StatsCmd
END TRY
BEGIN CATCH
END CATCH;

BEGIN TRY
  DBCC TRACEOFF(2388) WITH NO_INFOMSGS;
END TRY
BEGIN CATCH
END CATCH;

SELECT *
INTO dbo.tmpIndexCheck36
FROM #TMP1
WHERE LeadingType = 'Ascending'

SELECT * FROM dbo.tmpIndexCheck36
ORDER BY current_number_of_rows_table DESC, 
         Database_Name,
         Schema_Name,
         Table_Name,
         Index_Name