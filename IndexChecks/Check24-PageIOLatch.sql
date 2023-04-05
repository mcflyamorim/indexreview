/* 
Check24 – Indexes waiting on physical disk I/O operations

Description:
SQL Server tracks how much time the Database Engine spend waiting to complete a physical disk I/O request. This information is stored at the index level access and can be used to identify what are the objects that are spending most of the time on disk I/O operations. Long I/O waits may indicate problems with the disk subsystem.

Estimated Benefit:
High

Estimated Effort:
High

Recommendation:
Quick recommendation:
Review TOP N indexes and work to minimize the physical disk I/O operations.

Detailed recommendation:
Long waits for disk I/O operations usually indicate the I/O subsystem is overloaded, but it is also very common that the problem is with SQL Server queries other than with the I/O subsystem. The question you need to ask is, “why SQL Server is doing so many reads?”. It is not unusual to see that, a missing index or an unexpected table scan are doing a very high number of I/O requests.
Apply index compression to increase chances of having the data in memory and avoid the physical reads.
Reduce index fragmentation and increase page density. Queries that read many pages can degrade query performance because number of I/O requests required to read the data. Instead of a high number of large I/O requests, a query using an optimized index would require a small number of I/O requests to read the same amount of data.
*/



SET NOCOUNT ON; SET ARITHABORT OFF; SET ARITHIGNORE ON; 
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
SET LOCK_TIMEOUT 60000; /*60 seconds*/
SET DATEFORMAT MDY

IF OBJECT_ID('tempdb.dbo.tmpIndexCheck24') IS NOT NULL
  DROP TABLE tempdb.dbo.tmpIndexCheck24

SELECT TOP 1000
       'Check24 – Indexes waiting on physical disk I/O operations' AS [Info],
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
       a.avg_fragmentation_in_percent,
       a.user_scans,
       a.range_scan_count,
       a.page_io_latch_wait_count,
       a.page_io_latch_wait_in_ms AS total_page_io_latch_wait_in_ms,
       CAST(1. * a.page_io_latch_wait_in_ms / NULLIF(a.page_io_latch_wait_count ,0) AS DECIMAL(12,2)) AS page_io_latch_avg_wait_ms,
       CONVERT(VARCHAR(200), ((page_io_latch_wait_in_ms) / 1000) / 86400) + 'd:' + CONVERT(VARCHAR(20), DATEADD(s, ((page_io_latch_wait_in_ms) / 1000), 0), 108) AS total_page_io_latch_wait_d_h_m_s
  INTO tempdb.dbo.tmpIndexCheck24
  FROM tempdb.dbo.Tab_GetIndexInfo a
 WHERE a.page_io_latch_wait_count > 0
   AND a.Table_Name NOT LIKE N'plan_%'
   AND a.Table_Name NOT LIKE N'sys%'
   AND a.Table_Name NOT LIKE N'xml_index_nodes%'
ORDER BY A.page_io_latch_wait_count DESC,
         a.Number_Rows DESC, 
         a.Database_Name,
         a.Schema_Name,
         a.Table_Name,
         a.ReservedSizeInMB DESC,
         a.Index_Name

SELECT * FROM tempdb.dbo.tmpIndexCheck24
ORDER BY page_io_latch_wait_count DESC,
         current_number_of_rows_table DESC, 
         Database_Name,
         Schema_Name,
         Table_Name,
         ReservedSizeInMB DESC,
         Index_Name