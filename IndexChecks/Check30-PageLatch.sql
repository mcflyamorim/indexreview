/* 
Check30 - Indexes waiting to access pages located in the buffer cache
Description:
As the number of CPU cores on servers continues to increase, the associated increase in concurrency can introduce contention points on data structures that must be accessed in a serial fashion within the database engine. This is especially true for high throughput/high concurrency transaction processing (OLTP) workloads.
SQL Server tracks how much time the Database Engine spend waiting to access pages located in the buffer cache. This information is stored at the index level access and can be used to identify what are the objects that are spending most of the time on physical access to database pages that are located in the buffer cache. 
Environments with long waits may experience a decrease in performance of application.

Estimated Benefit:
High

Estimated Effort:
High

Recommendation:
Quick recommendation:
Review TOP N indexes and work to minimize time spent to access data pages.

Detailed recommendation:
Consider the following options to resolve the issue:
Prevent all concurrent INSERT operations from accessing the same database page. Instead, make each INSERT operation access a different page and increase concurrency. Therefore, any of the following methods that organize the data by a column other than the sequential column achieves this goal.
Use OPTIMIZE_FOR_SEQUENTIAL_KEY index option (SQL Server 2019 only).
Move primary key off identity column. Make the column that contains sequential values a nonclustered index, and then move the clustered index to another column.
Make the leading key a non-sequential column.
Add a non-sequential value as a leading key.
Use a GUID as a leading key.
Use table partitioning and a computed column with a hash value.
Switch to In-Memory OLTP.

https://learn.microsoft.com/en-us/sql/relational-databases/diagnose-resolve-latch-contention?view=sql-server-ver16
https://learn.microsoft.com/en-us/troubleshoot/sql/database-engine/performance/resolve-pagelatch-ex-contention
*/



SET NOCOUNT ON; SET ARITHABORT OFF; SET ARITHIGNORE ON; 
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
SET LOCK_TIMEOUT 60000; /*60 seconds*/
SET DATEFORMAT MDY

IF OBJECT_ID('tempdb.dbo.tmpIndexCheck30') IS NOT NULL
  DROP TABLE tempdb.dbo.tmpIndexCheck30

SELECT TOP 1000
       'Check 30 - Indexes waiting for PAGELATCH' AS [Info],
       a.Database_Name,
       a.Schema_Name,
       a.Table_Name,
       a.Index_Name,
       a.Index_Type,
       a.Indexed_Columns,
       a.Number_Rows AS current_number_of_rows_table,
       a.ReservedSizeInMB,
       user_seeks + user_scans + user_lookups + user_updates AS number_of_access_on_index_table_since_last_restart_or_rebuild,
       a.last_datetime_obj_was_used,
       a.page_latch_wait_count,
       a.page_latch_wait_in_ms AS total_page_latch_wait_in_ms,
       CAST(1. * a.page_latch_wait_in_ms / NULLIF(a.page_latch_wait_count ,0) AS decimal(12,2)) AS page_latch_avg_wait_ms,
       CONVERT(VARCHAR(10), ((page_latch_wait_in_ms) / 1000) / 86400) + 'd:' + CONVERT(VARCHAR(20), DATEADD(s, ((page_latch_wait_in_ms) / 1000), 0), 108) AS total_page_latch_wait_d_h_m_s
  INTO tempdb.dbo.tmpIndexCheck30
  FROM tempdb.dbo.Tab_GetIndexInfo a
 WHERE a.page_latch_wait_count > 0
   AND a.Table_Name NOT LIKE N'plan_%'
   AND a.Table_Name NOT LIKE N'sys%'
   AND a.Table_Name NOT LIKE N'xml_index_nodes%'
ORDER BY A.page_latch_wait_count DESC,
         a.Number_Rows DESC, 
         a.Database_Name,
         a.Schema_Name,
         a.Table_Name,
         a.ReservedSizeInMB DESC,
         a.Index_Name

SELECT * FROM tempdb.dbo.tmpIndexCheck30
ORDER BY page_latch_wait_count DESC,
         current_number_of_rows_table DESC, 
         Database_Name,
         Schema_Name,
         Table_Name,
         ReservedSizeInMB DESC,
         Index_Name
