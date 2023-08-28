/* 
Check10 - Unused or rarely used indexes

Description:
While proper indexes can greatly improve the performance, they can also negatively impact UPDATE, DELETE and INSERT operations. They also have a resource cost to maintain and can increase disk space consumption. Unused indexes have all these costs without providing any benefit. 
Unused indexes can slow down database's performance. Time of write operations is increased because of index maintenance, but index is not used anywhere.
Note: We are considering indexes that are not part of a primary key or unique constraint and haven't been used since the last SQL restart. Index usage statistics are reset at each SQL restart, so it is best to collect these after the server has been running for some time.

Estimated Benefit:
Medium

Estimated Effort:
Very High

Recommendation:
Quick recommendation:
Remove unused indexes.

Detailed recommendation:
Review the unused or rarely used indexes and drop them after confirm they’re unnecessary.
*/



SET NOCOUNT ON; SET ARITHABORT OFF; SET ARITHIGNORE ON; 
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
SET LOCK_TIMEOUT 60000; /*60 seconds*/
SET DATEFORMAT MDY

IF OBJECT_ID('tempdb.dbo.tmpIndexCheck10') IS NOT NULL
  DROP TABLE tempdb.dbo.tmpIndexCheck10

SELECT 'Check 10 - Unused or rarely used indexes' AS [Info],
        a.Database_Name,
        a.Schema_Name,
        a.Table_Name,
        a.Index_Name,
        a.Index_Type,
        a.indexed_columns,
        a.Number_Rows AS current_number_of_rows_table,
        a.ReservedSizeInMB,
        Tab1.[Reads_Ratio],
	       Tab2.[Writes_Ratio],
        ISNULL(Number_of_Reads,0) AS [Number of reads], 
        ISNULL([Total Writes],0) AS [Number of writes], 
        user_seeks + user_scans + user_lookups + user_updates AS number_of_access_on_index_table_since_last_restart_or_rebuild,
        a.last_datetime_obj_was_used,
        tab3.avg_of_access_per_minute_based_on_index_usage_dmv,
        a.plan_cache_reference_count,
        CASE 
          WHEN ISNULL(Number_of_Reads,0) = 0 AND ISNULL([Total Writes],0) > 0 THEN '[Unused index with update. It is recommended to revise the need to maintain all these objects as soon as possible]'
          ELSE  'OK'
        END AS [Comment 1],
        CASE 
          WHEN ISNULL(Number_of_Reads,0) = 0 AND ISNULL([Total Writes],0) = 0 THEN '[Unused index without update. It is recommended to revise the need to maintain all these objects as soon as possible]'
          ELSE  'OK'
        END AS [Comment 2],
        CASE 
          WHEN ISNULL(Number_of_Reads,0) > 0 AND [Reads_Ratio] < 5 THEN '[Rarely used index. It is recommended to revise the need to maintain all these objects as soon as possible]'
          ELSE  'OK'
        END AS [Comment 3]
   INTO tempdb.dbo.tmpIndexCheck10
   FROM tempdb.dbo.Tab_GetIndexInfo AS a
   CROSS APPLY (SELECT ISNULL(CONVERT(NUMERIC(18, 2),CAST(CASE WHEN (a.user_seeks + a.user_scans + a.user_lookups) = 0 THEN 0 ELSE CONVERT(REAL, (a.user_seeks + a.user_scans + a.user_lookups)) * 100 /
              		       CASE (a.user_seeks + a.user_scans + a.user_lookups + a.user_updates) WHEN 0 THEN 1 ELSE CONVERT(REAL, (a.user_seeks + a.user_scans + a.user_lookups + a.user_updates)) END END AS DECIMAL(18,2))),0)) AS Tab1([Reads_Ratio])
   CROSS APPLY (SELECT ISNULL(CONVERT(NUMERIC(18, 2),CAST(CASE WHEN a.user_updates = 0 THEN 0 ELSE CONVERT(REAL, a.user_updates) * 100 /
		                     CASE (a.user_seeks + a.user_scans + a.user_lookups + a.user_updates) WHEN 0 THEN 1 ELSE CONVERT(REAL, (a.user_seeks + a.user_scans + a.user_lookups + a.user_updates)) END END AS DECIMAL(18,2))),0)) AS Tab2([Writes_Ratio])
   CROSS APPLY (SELECT CONVERT(VARCHAR(200), user_seeks + user_scans + user_lookups + user_updates / 
                                CASE DATEDIFF(mi, (SELECT create_date FROM sys.databases WHERE name = 'tempdb'), GETDATE())
                                  WHEN 0 THEN 1
                                  ELSE DATEDIFF(mi, (SELECT create_date FROM sys.databases WHERE name = 'tempdb'), GETDATE())
                                END)) AS tab3(avg_of_access_per_minute_based_on_index_usage_dmv)
SELECT * FROM tempdb.dbo.tmpIndexCheck10
 ORDER BY current_number_of_rows_table DESC, 
          Database_Name,
          Schema_Name,
          Table_Name,
          ReservedSizeInMB DESC,
          Index_Name