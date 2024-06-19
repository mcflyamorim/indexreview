/* 
Check19 - Heaps

Description:
Poorly designed indexes and a lack of indexes are primary sources of database application bottlenecks. Designing efficient indexes is paramount to achieving good database and application performance.
When a table has a clustered index, the table is called a clustered table. If a table has no clustered index, its data rows are stored in an unordered structure called a heap. If a table is a heap and does not have any nonclustered indexes, then the entire table must be read (a table scan) to find any row.

Estimated Benefit:
High

Estimated Effort:
Low

Recommendation:
Quick recommendation:
Create appropriate indexes to improve performance of queries.

Detailed recommendation:
There are sometimes good reasons to leave a table as a heap instead of creating a clustered index, but using heaps effectively is an advanced skill. Most tables should have a carefully chosen clustered index unless a good reason exists for leaving the table as a heap.
Review all tables with no indexes and make sure they’re really used.
Create appropriate indexes to improve performance of queries.
Usually, some acceptable usages for heaps are:
•	Heaps can be used as staging tables for large, unordered insert operations.
•	Sometimes data professionals also use heaps when data is always accessed through nonclustered indexes, and the RID is smaller than a clustered index key.
•	Very small tables.
*/



SET NOCOUNT ON; SET ARITHABORT OFF; SET ARITHIGNORE ON; 
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
SET LOCK_TIMEOUT 60000; /*60 seconds*/
SET DATEFORMAT MDY

IF OBJECT_ID('dbo.tmpIndexCheck19') IS NOT NULL
  DROP TABLE dbo.tmpIndexCheck19
 
SELECT 'Check19 - Heaps' AS [Info],
       a.Database_Name,
       a.Schema_Name,
       a.Table_Name,
       a.Index_Name,
       a.Index_Type,
       a.Number_Rows AS current_number_of_rows_table,
       a.ReservedSizeInMB AS [Table Size],
       a.plan_cache_reference_count,
       user_seeks + user_scans + user_lookups + user_updates AS number_of_access_on_index_table_since_last_restart_or_rebuild,
       a.last_datetime_obj_was_used,
       a.user_scans,
       a.user_lookups,
       a.singleton_lookup_count,
       a.range_scan_count,
       a.page_io_latch_wait_count,
       a.page_io_latch_wait_in_ms AS total_page_io_latch_wait_in_ms,
       CAST(1. * a.page_io_latch_wait_in_ms / NULLIF(a.page_io_latch_wait_count ,0) AS decimal(12,2)) AS page_io_latch_avg_wait_ms
  INTO dbo.tmpIndexCheck19
  FROM dbo.Tab_GetIndexInfo a
 WHERE a.Index_Type = 'HEAP'

SELECT * FROM dbo.tmpIndexCheck19
ORDER BY current_number_of_rows_table DESC, 
         Database_Name,
         Schema_Name,
         Table_Name,
         Index_Name