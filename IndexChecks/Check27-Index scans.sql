/* 
Check27 - Index scans

Description:
Reports the TOP indexes and number of scans by user queries that did not use 'seek' predicate. Normally the index seek is faster than index scan since a scan reads all the rows in an index - B-tree in the index order whereas index seek traverses a B-tree and walks through leaf nodes seeking only the matching or qualifying rows based on the filter criteria.

Estimated Benefit:
Medium

Estimated Effort:
High

Recommendation:
Quick recommendation:
Review queries using the indexes and optimize it.

Detailed recommendation:
Identify the queries doing the scans and work in a performance tuning review to improve it and create the necessary indexes to solve the problem.
The plan cache and/or query store can be used to find the plans doing the scan.

Note: Good candidates for a columnstore and batch mode.
*/



SET NOCOUNT ON; SET ARITHABORT OFF; SET ARITHIGNORE ON; 
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
SET LOCK_TIMEOUT 60000; /*60 seconds*/
SET DATEFORMAT MDY

IF OBJECT_ID('tempdb.dbo.tmpIndexCheck27') IS NOT NULL
  DROP TABLE tempdb.dbo.tmpIndexCheck27

SELECT TOP 1000
       'Check27 - Index scans' AS [Info],
       a.Database_Name,
       a.Schema_Name,
       a.Table_Name,
       a.Index_Name,
       a.Index_Type,
       a.indexed_columns,
       a.Number_Rows AS current_number_of_rows_table,
       a.data_compression_desc,
       a.ReservedSizeInMB,
       user_seeks + user_scans + user_lookups + user_updates AS number_of_access_on_index_table_since_last_restart_or_rebuild,
       a.last_datetime_obj_was_used,
       a.plan_cache_reference_count,
       a.Buffer_Pool_SpaceUsed_MB,
       a.avg_fragmentation_in_percent,
       a.user_scans,
       a.range_scan_count,
       a.page_io_latch_wait_count,
       a.page_io_latch_wait_in_ms AS total_page_io_latch_wait_in_ms,
       CAST(1. * a.page_io_latch_wait_in_ms / NULLIF(a.page_io_latch_wait_count ,0) AS decimal(12,2)) AS page_io_latch_avg_wait_ms
  INTO tempdb.dbo.tmpIndexCheck27
  FROM tempdb.dbo.Tab_GetIndexInfo a
WHERE a.user_scans > 0 OR a.range_scan_count > 0
ORDER BY a.user_scans DESC,
         a.Number_Rows DESC, 
         a.Database_Name,
         a.Schema_Name,
         a.Table_Name,
         a.ReservedSizeInMB DESC,
         a.Index_Name

SELECT * FROM tempdb.dbo.tmpIndexCheck27
ORDER BY user_scans DESC,
         current_number_of_rows_table DESC, 
         Database_Name,
         Schema_Name,
         Table_Name,
         ReservedSizeInMB DESC,
         Index_Name
