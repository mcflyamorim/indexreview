/* 
Check37 - Indexes and lock wait time

Description:
Queries using indexes with high wait on lock operations can experience degraded concurrency and performance.

Estimated Benefit:
Medium

Estimated Effort:
High

Recommendation:
Quick recommendation:
Review reported indexes and check if queries using those indexes are experiencing unexpected blocking and work to resolve it.

Detailed recommendation:
Identify queries holding locks on the reported indexes and causing blocks and work to reduce the query's lock footprint by making the query as efficient as possible. Large scans or many bookmark lookups can increase the chance of blocking problems. Additionally, these increase the chance of deadlocks, and adversely affect concurrency and performance. Review the execution plan and potentially create new nonclustered indexes to improve query performance.
Avoid to use lock table hints and review transaction isolation level to confirm this is using the correct option.
Consider to implement snapshot isolation level.
*/



SET NOCOUNT ON; SET ARITHABORT OFF; SET ARITHIGNORE ON; 
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
SET LOCK_TIMEOUT 60000; /*60 seconds*/
SET DATEFORMAT MDY

IF OBJECT_ID('dbo.tmpIndexCheck37') IS NOT NULL
  DROP TABLE dbo.tmpIndexCheck37

SELECT TOP 1000
       'Check37 - Indexes and lock wait time' AS [Info],
       a.Database_Name,
       a.Schema_Name,
       a.Table_Name,
       a.Index_Name,
       a.Index_Type,
       a.indexed_columns,
       a.Number_Rows AS current_number_of_rows_table,
       a.ReservedSizeInMB AS [Table Size],
       a.row_lock_wait_count,
       a.row_lock_wait_in_ms AS total_row_lock_wait_in_ms,
       CAST(1. * a.row_lock_wait_in_ms / NULLIF(a.row_lock_wait_count ,0) AS decimal(12,2)) AS avg_row_lock_wait_in_ms,
       CONVERT(VARCHAR(200), ((row_lock_wait_in_ms) / 1000) / 86400) + 'd:' + CONVERT(VARCHAR(20), DATEADD(s, ((row_lock_wait_in_ms) / 1000), 0), 108) AS total_row_lock_duration_d_h_m_s,
       a.page_lock_count,
       a.page_lock_wait_in_ms AS total_page_lock_wait_in_ms,
       CAST(1. * a.page_lock_wait_in_ms / NULLIF(a.page_lock_count ,0) AS decimal(12,2)) AS avg_page_lock_wait_in_ms,
       CONVERT(VARCHAR(200), ((page_lock_wait_in_ms) / 1000) / 86400) + 'd:' + CONVERT(VARCHAR(20), DATEADD(s, ((page_lock_wait_in_ms) / 1000), 0), 108) AS total_page_lock_duration_d_h_m_s
  INTO dbo.tmpIndexCheck37
  FROM dbo.Tab_GetIndexInfo a
 WHERE a.row_lock_wait_count + a.page_lock_count > 0
ORDER BY row_lock_wait_count + page_lock_count DESC,
         ReservedSizeInMB DESC, 
         Database_Name,
         Schema_Name,
         Table_Name,
         Index_Name

SELECT * FROM dbo.tmpIndexCheck37
ORDER BY row_lock_wait_count + page_lock_count DESC,
         [Table Size] DESC, 
         Database_Name,
         Schema_Name,
         Table_Name,
         Index_Name
