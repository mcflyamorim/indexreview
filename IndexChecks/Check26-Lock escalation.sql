/* 
Check26 - Lock escalation

Description:
Lock escalation is the process of converting many fine-grained locks (such as row or page locks) to table locks. SQL Server dynamically determines when to do lock escalation. When it makes this decision, SQL Server considers the number of locks that are held on a particular scan, the number of locks that are held by the whole transaction, and the memory that's used for locks in the system as a whole.

Estimated Benefit:
Medium

Estimated Effort:
High

Recommendation:
Quick recommendation:
Check if lock escalation is causing unexpected blocking and work to resolve it.

Detailed recommendation:
Review TOP N indexes by number of lock escalations and make sure this is expected and it is not blocking other users trying to access the index.
Some application or query designs might trigger lock escalation at a time when this action not desirable, and the escalated table lock might block other users. To determine whether lock escalation is occurring at or near the time when you experience blocking issues, start an Extended Events session that includes the lock_escalation event.
The simplest and safest method to prevent lock escalation is to keep transactions short and reduce the lock footprint of expensive queries so that the lock escalation thresholds are not exceeded.
Reduce the query's lock footprint by making the query as efficient as possible. Large scans or many bookmark lookups can increase the chance of lock escalation. Additionally, these increase the chance of deadlocks, and adversely affect concurrency and performance. Review the execution plan and potentially create new nonclustered indexes to improve query performance.
Although it's possible to disable lock escalation in SQL Server, we recommend it to be used only as a last resort by experienced developers and database administrators.
*/



SET NOCOUNT ON; SET ARITHABORT OFF; SET ARITHIGNORE ON; 
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
SET LOCK_TIMEOUT 60000; /*60 seconds*/
SET DATEFORMAT MDY

IF OBJECT_ID('tempdb.dbo.tmpIndexCheck26') IS NOT NULL
  DROP TABLE tempdb.dbo.tmpIndexCheck26

SELECT TOP 1000
       'Check26 - Lock escalation' AS [Info],
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
       a.allow_row_locks,
       a.allow_page_locks,
       a.lock_escalation_desc,
       a.index_lock_escaltion_count,
       a.index_lock_escaltion_attempt_count,
       a.row_lock_wait_count,
       a.row_lock_wait_in_ms,
       a.page_lock_count,
       a.page_lock_wait_in_ms
  INTO tempdb.dbo.tmpIndexCheck26
  FROM tempdb.dbo.Tab_GetIndexInfo a
WHERE a.index_lock_escaltion_count > 0
ORDER BY a.index_lock_escaltion_count DESC,
         a.Number_Rows DESC, 
         a.Database_Name,
         a.Schema_Name,
         a.Table_Name,
         a.ReservedSizeInMB DESC,
         a.Index_Name

SELECT * FROM tempdb.dbo.tmpIndexCheck26
ORDER BY index_lock_escaltion_count DESC,
         current_number_of_rows_table DESC, 
         Database_Name,
         Schema_Name,
         Table_Name,
         ReservedSizeInMB DESC,
         Index_Name
