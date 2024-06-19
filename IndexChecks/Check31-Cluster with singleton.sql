/* 
Check31 - Most accessed indexes and number of singleton lookups and range scans

Description:
Reports the TOP most accessed indexes and number of singleton lookups and range scans. If most of reads are singleton lookups, it may be a good idea to re-create the index as a nonclustered and use cluster in a column that requires range scans.

Estimated Benefit:
Medium

Estimated Effort:
High

Recommendation:
Quick recommendation:
Review the cluster index access and check if it is worthy to re-create it as a nonclustered.

Detailed recommendation:
One of most important aspects of a clustered index, is that all columns of a table are stored on it. This means that it is very good for supporting range queries and reading several columns.
Although a clustered is often (because it is usually defined as the primary key) used to return a single row (a singleton lookup) from the index, it could be better used on range queries and queries reading several columns. 
If most of access to the clustered index is returning a single row, consider to re-create it as a non-clustered and define the clustered in another column (or an existing non-clustered index) that is used on range filters and/or return several columns. The non-clustered indexes are considered better (and good candidates to become clustered indexes) compared to the existing clustered indexes if the number of user seeks on those indexes is greater than the number of lookups on the related to the table clustered index.
*/



SET NOCOUNT ON; SET ARITHABORT OFF; SET ARITHIGNORE ON; 
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
SET LOCK_TIMEOUT 60000; /*60 seconds*/
SET DATEFORMAT MDY

IF OBJECT_ID('dbo.tmpIndexCheck31') IS NOT NULL
  DROP TABLE dbo.tmpIndexCheck31

SELECT TOP 1000
       'Check31 - Most accessed indexes and number of singleton lookups and range scans' AS [Info],
       a.Database_Name,
       a.Schema_Name,
       a.Table_Name,
       a.Index_Name,
       a.Index_Type,
       a.indexed_columns,
       a.Number_Rows AS current_number_of_rows_table,
       user_seeks + user_scans + user_lookups + user_updates AS number_of_access_on_index_table_since_last_restart_or_rebuild,
       a.last_datetime_obj_was_used,
       a.ReservedSizeInMB,
       a.singleton_lookup_count,
       a.range_scan_count,
       Tab1.singleton_lookup_ratio,
       Tab2.range_scan_ratio,
       a.user_seeks,
       a.user_scans,
       a.user_lookups,
       a.Number_of_Reads
  INTO dbo.tmpIndexCheck31
  FROM dbo.Tab_GetIndexInfo a
   CROSS APPLY (SELECT ISNULL(CONVERT(NUMERIC(18, 2),CAST(CASE WHEN (singleton_lookup_count) = 0 THEN 0 ELSE CONVERT(REAL, (singleton_lookup_count)) * 100 /
              		       CASE (singleton_lookup_count + range_scan_count) WHEN 0 THEN 1 ELSE CONVERT(REAL, (singleton_lookup_count + range_scan_count)) END END AS DECIMAL(18,2))),0)) AS Tab1(singleton_lookup_ratio)
   CROSS APPLY (SELECT ISNULL(CONVERT(NUMERIC(18, 2),CAST(CASE WHEN range_scan_count = 0 THEN 0 ELSE CONVERT(REAL, range_scan_count) * 100 /
		                     CASE (singleton_lookup_count + range_scan_count) WHEN 0 THEN 1 ELSE CONVERT(REAL, (singleton_lookup_count + range_scan_count)) END END AS DECIMAL(18,2))),0)) AS Tab2(range_scan_ratio)
ORDER BY a.Number_of_Reads DESC,
         a.Number_Rows DESC, 
         a.Database_Name,
         a.Schema_Name,
         a.Table_Name,
         a.ReservedSizeInMB DESC,
         a.Index_Name

SELECT * FROM dbo.tmpIndexCheck31
ORDER BY current_number_of_rows_table DESC, 
         Index_Type,
         Database_Name,
         Schema_Name,
         Table_Name,
         ReservedSizeInMB DESC,
         Index_Name